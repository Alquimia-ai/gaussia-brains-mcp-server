"""REST: the brain lifecycle over the working store (``/v1/brains``, ``/v1/jobs``).

Reads (status, browse, history, search) need ``brains:catalog``, ``brains:read``
or ``brains:write``; everything that changes a brain needs ``brains:write`` and
is attributed to the token's human subject. Push, pull, converge and import
answer ``202`` with a job (``GET /v1/jobs/{id}``).

Errors carry a stable ``code`` (see :mod:`gaussia_brains_mcp_server.lifecycle.errors`)::

    {"code": "not_fast_forward", "message": "...", "detail": {...}}
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field

from gaussia_brains_mcp_server.auth.http import require_any_scope, require_scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import CATALOG, READ, WRITE
from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.lifecycle.errors import (
    ALL_CODES,
    REFUSED_POLICY,
    USAGE,
    LifecycleError,
    describe,
)
from gaussia_brains_mcp_server.lifecycle.identity import actor_for
from gaussia_brains_mcp_server.lifecycle.jobs import Job
from gaussia_brains_mcp_server.lifecycle.service import (
    LifecycleService,
    search_registry,
)


class LifecycleRoute(APIRoute):
    """Turns any error raised by a lifecycle handler into its stable error body."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def route(request: Request) -> Response:
            try:
                return await handler(request)
            except HTTPException:
                raise
            except Exception as exc:  # noqa: BLE001 - mapped to a stable code
                info = describe(exc)
                if info.code == "internal":
                    from loguru import logger

                    logger.exception("{} {} failed", request.method, request.url.path)
                return JSONResponse(status_code=info.status, content=info.body())

        return route


router = APIRouter(prefix="/v1/brains", tags=["lifecycle"], route_class=LifecycleRoute)
jobs_router = APIRouter(prefix="/v1/jobs", tags=["jobs"], route_class=LifecycleRoute)

ReadPrincipal = Annotated[Principal, Depends(require_any_scope(CATALOG, READ, WRITE))]
SearchPrincipal = Annotated[Principal, Depends(require_any_scope(READ, CATALOG, WRITE))]
WritePrincipal = Annotated[Principal, Depends(require_scopes(WRITE))]
IfMatch = Annotated[
    str | None,
    Header(
        alias="If-Match",
        description="The branch head the client last read; the write is refused "
        "with not_fast_forward if the head moved since.",
    ),
]
BranchQuery = Annotated[str | None, Query(description="Branch to read (default: main)")]


# --------------------------------------------------------------------------- #
# Schemas (the OpenAPI contract)
# --------------------------------------------------------------------------- #


class ErrorBody(BaseModel):
    code: Literal[ALL_CODES]  # type: ignore[valid-type]
    message: str
    detail: dict[str, Any] = Field(default_factory=dict)


ERRORS: dict[int | str, dict[str, Any]] = {
    status: {"model": ErrorBody} for status in (400, 403, 404, 409, 422, 502)
}


class JobView(BaseModel):
    id: str
    kind: Literal["push", "pull", "converge", "import"]
    brain: str
    actor: str
    state: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    stage: str
    cancellable: bool
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


class CreateBrainRequest(BaseModel):
    reference: str = Field(
        description="Brain reference, short (`brains/handbook`) or full "
        "(`registry.example.com/acme/handbook`). A `:tag` sets the default tag."
    )
    default_tag: str | None = Field(
        default=None, description="The tag main publishes to (default `latest`)."
    )
    title: str = ""
    description: str = ""


class CandidateIn(BaseModel):
    memory_type: Literal["episodic", "semantic", "procedural"]
    payload: dict[str, Any]
    evidence: list[str] = Field(min_length=1)
    locator: str | None = None
    confidence: str | None = None


class ProducerIn(BaseModel):
    kind: Literal["model", "pipeline", "batch", "actor"] = "model"
    id: str = Field(
        description="The model as `namespace/model`, e.g. `openai/gpt-4.1`."
    )
    version: str | None = None


class CollaboratorIn(BaseModel):
    id: str = Field(
        description="An address or a namespaced name, e.g. `alquimia/ledger`."
    )
    kind: Literal["human", "agent", "service", "pipeline"] = "agent"
    name: str | None = None
    model: str | None = Field(default=None, description="The model this party ran.")


class CommitRequest(BaseModel):
    source: str = Field(
        description="The canonical evidence block the candidates derive from."
    )
    candidates: list[CandidateIn] = Field(min_length=1)
    producer: ProducerIn | None = Field(
        default=None, description="What proposed the candidates (the model)."
    )
    assisted_by: list[CollaboratorIn] = Field(default_factory=list)
    allowed: list[Literal["episodic", "semantic", "procedural"]] | None = None
    requirements: list[str] | None = None
    instructions: str | None = None
    task_id: str | None = None
    dry_run: bool = False
    branch: str | None = None


class SearchRequest(BaseModel):
    query: str
    memory_types: list[str] | None = None
    limit: int = Field(default=10, ge=1, le=100)
    expand_depth: int = Field(default=0, ge=0, le=3)
    mode: Literal["auto", "exact", "lexical", "semantic", "associative"] = "auto"
    include_unverified: bool = False
    branch: str | None = None


class CreateBranchRequest(BaseModel):
    name: str = Field(
        description="Segments of letters, digits, `_` and `-` joined by `/`."
    )
    start: str | None = Field(
        default=None,
        description="Another branch, a `sha256:` snapshot, or the main head.",
    )


class PushRequest(BaseModel):
    force: bool = Field(
        default=False, description="Overwrite a diverged remote, discarding its work."
    )


class PullRequest(BaseModel):
    branch: str | None = Field(
        default=None, description="Pull the tag this branch publishes to."
    )
    tag: str | None = Field(default=None, description="Pull this tag instead.")


class ConvergeRequest(BaseModel):
    from_: str = Field(
        alias="from",
        description="A local branch, a tag of this brain's repository, or `registry/repo:tag`.",
    )
    into: str = "main"
    strategy: Literal["merge", "rebase", "squash"] | None = Field(
        default=None,
        description="Required when the histories diverged; never defaulted.",
    )
    reason: str | None = None
    fast_forward: Literal["auto", "only", "never"] = "auto"

    model_config = {"populate_by_name": True}


class ResolutionRequest(BaseModel):
    block: str
    kind: Literal["admit", "reject", "prefer"]
    prefer: str | None = None
    reason: str | None = None


class ReasonRequest(BaseModel):
    reason: str | None = None


class ImportRequest(BaseModel):
    source: str | None = Field(
        default=None,
        description="`registry/repo:tag` to import from; default the brain's own reference.",
    )
    tag: str | None = None
    default_tag: str | None = None
    title: str = ""
    description: str = ""


# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #


def get_service(request: Request) -> LifecycleService:
    service = getattr(request.app.state, "lifecycle", None)
    if service is None:
        raise LifecycleError(
            REFUSED_POLICY,
            "this replica holds no working store (GBMS_STORE_ROOT is unset); "
            "writes go to the writer replica",
        )
    return service


Service = Annotated[LifecycleService, Depends(get_service)]


def _job_response(job: Job) -> JSONResponse:
    return JSONResponse(
        status_code=202, content=job.view(), headers={"Location": f"/v1/jobs/{job.id}"}
    )


# --------------------------------------------------------------------------- #
# Create, status, browse
# --------------------------------------------------------------------------- #


@router.post("", status_code=201, responses=ERRORS)
async def create_brain(
    body: CreateBrainRequest,
    principal: WritePrincipal,
    service: Service,
    response: Response,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Create an empty, ungoverned brain in the working store.

    Its first evidence or commit creates its history.
    """
    status = service.create(
        body.reference,
        actor=actor_for(principal, settings),
        default_tag=body.default_tag,
        title=body.title,
        description=body.description,
    )
    response.headers["Location"] = f"/v1/brains/{status['reference']}"
    return status


@router.get("/{brain_ref:path}/modules/{memory_type}/blocks", responses=ERRORS)
async def list_blocks(
    brain_ref: str,
    memory_type: str,
    principal: ReadPrincipal,
    service: Service,
    branch: BranchQuery = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    """Blocks of one module, newest first; superseded and unreadable ones included."""
    handle = service.handle(brain_ref)
    return await asyncio.to_thread(
        service.list_blocks,
        handle,
        memory_type,
        branch=branch,
        cursor=cursor,
        limit=limit,
    )


@router.get("/{brain_ref:path}/blocks/{block_id}", responses=ERRORS)
async def get_block(
    brain_ref: str,
    block_id: str,
    principal: ReadPrincipal,
    service: Service,
    branch: BranchQuery = None,
) -> dict[str, Any]:
    """One block: payload, extracted text, provenance, supersession, verification."""
    handle = service.handle(brain_ref)
    return await asyncio.to_thread(
        service.block_detail, handle, block_id, branch=branch
    )


@router.get("/{brain_ref:path}/history", responses=ERRORS)
async def get_history(
    brain_ref: str,
    principal: ReadPrincipal,
    service: Service,
    branch: BranchQuery = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    """The branch's snapshots, newest first (first-parent chain)."""
    handle = service.handle(brain_ref)
    return await asyncio.to_thread(
        service.history, handle, branch=branch, cursor=cursor, limit=limit
    )


@router.post("/{brain_ref:path}/search", responses=ERRORS)
async def search(
    brain_ref: str,
    body: SearchRequest,
    principal: SearchPrincipal,
    request: Request,
) -> dict[str, Any]:
    """Search with the engine MCP uses; reports truncation and narrowing.

    A brain held in the working store is searched at a branch head; any other
    servable brain is searched through the read cache, like MCP.
    """
    params = {
        "memory_types": body.memory_types,
        "limit": body.limit,
        "expand_depth": body.expand_depth,
        "mode": body.mode,
        "include_unverified": body.include_unverified,
    }
    service: LifecycleService | None = getattr(request.app.state, "lifecycle", None)
    if service is not None:
        handle = service.held(brain_ref)
        if handle is not None:
            return await asyncio.to_thread(
                service.search, handle, body.query, branch=body.branch, **params
            )
    return await search_registry(
        request.app.state.settings,
        request.app.state.brain_cache,
        brain_ref,
        body.query,
        **params,
    )


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #


@router.post("/{brain_ref:path}/evidence", status_code=201, responses=ERRORS)
async def register_evidence(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    file: Annotated[UploadFile, File(description="The source, byte for byte.")],
    media_type: Annotated[str | None, Form()] = None,
    origin: Annotated[str | None, Form()] = None,
    license: Annotated[str | None, Form()] = None,
    branch: Annotated[str | None, Form()] = None,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Register a source as canonical evidence (multipart). Identical bytes are a no-op."""
    limit = settings.store_max_evidence_bytes
    data = await file.read(limit + 1)
    if len(data) > limit:
        raise LifecycleError(USAGE, f"the evidence is larger than {limit} bytes")
    handle = service.handle(brain_ref)
    return await service.register_evidence(
        handle,
        data,
        actor=actor_for(principal, settings),
        media_type=media_type or file.content_type or "application/octet-stream",
        origin=origin,
        license=license,
        branch=branch,
        if_match=if_match,
    )


@router.post("/{brain_ref:path}/commits", responses=ERRORS)
async def commit(
    brain_ref: str,
    body: CommitRequest,
    principal: WritePrincipal,
    service: Service,
    response: Response,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Validate the client's candidates and commit them, all or nothing.

    ``dry_run`` returns every verdict and writes nothing. A commit whose
    candidates did not all validate is ``rejected_candidates`` (422) with the
    report; otherwise the committed blocks and the new snapshot.
    """
    handle = service.handle(brain_ref)
    result = await service.commit(
        handle,
        actor=actor_for(principal, settings),
        source=body.source,
        candidates=[c.model_dump(exclude_none=True) for c in body.candidates],
        producer=body.producer.model_dump(exclude_none=True) if body.producer else None,
        allowed=list(body.allowed) if body.allowed is not None else None,
        requirements=body.requirements,
        instructions=body.instructions,
        task_id=body.task_id,
        assisted_by=[c.model_dump(exclude_none=True) for c in body.assisted_by],
        dry_run=body.dry_run,
        branch=body.branch,
        if_match=if_match,
    )
    if result.get("head"):
        response.headers["ETag"] = f'"{result["head"]}"'
    if not body.dry_run:
        response.status_code = 201
    return result


# --------------------------------------------------------------------------- #
# Branches and distribution
# --------------------------------------------------------------------------- #


@router.get("/{brain_ref:path}/branches", responses=ERRORS)
async def list_branches(
    brain_ref: str,
    principal: ReadPrincipal,
    service: Service,
    remote: Annotated[
        bool, Query(description="Also list branches the registry publishes")
    ] = False,
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await service.list_branches(handle, remote=remote)


@router.post(
    "/{brain_ref:path}/branches/{name:path}/push", status_code=202, responses=ERRORS
)
async def push_branch(
    brain_ref: str,
    name: str,
    principal: WritePrincipal,
    service: Service,
    body: PushRequest | None = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> JSONResponse:
    """Push a branch to its tag (main → the default tag, others → `br.<name>`).

    A push of main also indexes the brain in its namespace.
    """
    handle = service.handle(brain_ref)
    job = service.push(
        handle,
        name,
        actor=actor_for(principal, settings),
        force=bool(body and body.force),
    )
    return _job_response(job)


@router.post("/{brain_ref:path}/branches", status_code=201, responses=ERRORS)
async def create_branch(
    brain_ref: str,
    body: CreateBranchRequest,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await service.create_branch(
        handle, body.name, actor=actor_for(principal, settings), start=body.start
    )


@router.delete("/{brain_ref:path}/branches/{name:path}", responses=ERRORS)
async def delete_branch(
    brain_ref: str,
    name: str,
    principal: WritePrincipal,
    service: Service,
    force: Annotated[bool, Query()] = False,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Delete a branch's local ref; its registry tag stays."""
    handle = service.handle(brain_ref)
    return await service.delete_branch(
        handle, name, actor=actor_for(principal, settings), force=force
    )


@router.post("/{brain_ref:path}/pull", status_code=202, responses=ERRORS)
async def pull(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: PullRequest | None = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> JSONResponse:
    handle = service.handle(brain_ref)
    job = service.pull(
        handle,
        actor=actor_for(principal, settings),
        tag=body.tag if body else None,
        branch=body.branch if body else None,
    )
    return _job_response(job)


# --------------------------------------------------------------------------- #
# Converge
# --------------------------------------------------------------------------- #


@router.post("/{brain_ref:path}/converge", status_code=202, responses=ERRORS)
async def converge(
    brain_ref: str,
    body: ConvergeRequest,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> JSONResponse:
    """Bring another branch or published tag into a branch (job).

    A convergence that stops to ask ends the job with ``needs_human``; the
    open convergence is then resolved through ``/convergence``.
    """
    handle = service.handle(brain_ref)
    job = service.converge(
        handle,
        actor=actor_for(principal, settings),
        source=body.from_,
        into=body.into,
        strategy=body.strategy,
        reason=body.reason,
        fast_forward=body.fast_forward,
    )
    return _job_response(job)


@router.get("/{brain_ref:path}/convergence", responses=ERRORS)
async def get_convergence(
    brain_ref: str, principal: ReadPrincipal, service: Service
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await asyncio.to_thread(service.convergence, handle)


@router.post("/{brain_ref:path}/convergence/resolutions", responses=ERRORS)
async def resolve(
    brain_ref: str,
    body: ResolutionRequest,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Answer one open question: admit, reject, or prefer a successor."""
    handle = service.handle(brain_ref)
    return await service.resolve(
        handle,
        actor=actor_for(principal, settings),
        block=body.block,
        kind=body.kind,
        prefer=body.prefer,
        reason=body.reason,
    )


@router.post("/{brain_ref:path}/convergence/accept-removals", responses=ERRORS)
async def accept_removals(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: ReasonRequest | None = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await service.accept_removals(
        handle,
        actor=actor_for(principal, settings),
        reason=body.reason if body else None,
    )


@router.post("/{brain_ref:path}/convergence/continue", responses=ERRORS)
async def continue_convergence(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Conclude a convergence whose questions are answered (``needs_human`` if not)."""
    handle = service.handle(brain_ref)
    return await service.continue_convergence(
        handle, actor=actor_for(principal, settings)
    )


@router.post("/{brain_ref:path}/convergence/abort", responses=ERRORS)
async def abort_convergence(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await service.abort_convergence(handle, actor=actor_for(principal, settings))


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #


@router.post(
    "/{brain_ref:path}/import",
    status_code=202,
    responses=ERRORS,
    openapi_extra={
        "requestBody": {
            "required": False,
            "content": {
                "application/json": {"schema": ImportRequest.model_json_schema()},
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "required": ["layout"],
                        "properties": {
                            "layout": {
                                "type": "string",
                                "format": "binary",
                                "description": "A .tar or .tar.gz of a brain's OCI layout.",
                            },
                            "default_tag": {"type": "string"},
                            "title": {"type": "string"},
                            "description": {"type": "string"},
                        },
                    }
                },
            },
        }
    },
)
async def import_brain(
    brain_ref: str,
    request: Request,
    principal: WritePrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> JSONResponse:
    """Migrate an existing brain into the working store (job).

    ``application/json`` (:class:`ImportRequest`) imports from the registry;
    ``multipart/form-data`` with a ``layout`` file (a ``.tar``/``.tar.gz`` of a
    brain's OCI layout) imports an upload. Governed brains are imported as they
    are and served read-only.
    """
    actor = actor_for(principal, settings)
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        upload = form.get("layout")
        if upload is None or isinstance(upload, str):
            raise LifecycleError(USAGE, "the form needs a `layout` file")
        archive = await _spool(upload, settings, service)
        job = service.import_layout(
            brain_ref,
            archive,
            actor=actor,
            default_tag=_form_str(form.get("default_tag")),
            title=_form_str(form.get("title")) or "",
            description=_form_str(form.get("description")) or "",
        )
        return _job_response(job)
    raw = await request.body()
    body = ImportRequest.model_validate_json(raw or b"{}")
    job = service.import_from_registry(
        brain_ref,
        actor=actor,
        tag=body.tag,
        source=body.source,
        default_tag=body.default_tag,
        title=body.title,
        description=body.description,
    )
    return _job_response(job)


def _form_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


async def _spool(upload: Any, settings: Settings, service: LifecycleService) -> Path:
    """Copy an upload to the store's volume, refusing it past the size limit."""
    limit = settings.store_max_import_bytes
    handle, path = tempfile.mkstemp(prefix=".upload-", dir=service.store.root)
    written = 0
    try:
        with open(handle, "wb") as out:
            while chunk := await upload.read(1024 * 1024):
                written += len(chunk)
                if written > limit:
                    raise LifecycleError(
                        USAGE, f"the upload is larger than {limit} bytes"
                    )
                out.write(chunk)
    except BaseException:
        Path(path).unlink(missing_ok=True)
        raise
    return Path(path)


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #


@jobs_router.get("", responses=ERRORS)
async def list_jobs(
    principal: WritePrincipal,
    service: Service,
    brain: Annotated[str | None, Query(description="Only jobs on this brain")] = None,
) -> dict[str, Any]:
    reference = service.repository(brain)[0] if brain else None
    return {"jobs": [job.view() for job in service.jobs.list(brain=reference)]}


@jobs_router.get("/{job_id}", response_model=JobView, responses=ERRORS)
async def get_job(
    job_id: str, principal: WritePrincipal, service: Service
) -> dict[str, Any]:
    return service.jobs.get(job_id).view()


@jobs_router.post("/{job_id}/cancel", response_model=JobView, responses=ERRORS)
async def cancel_job(
    job_id: str, principal: WritePrincipal, service: Service
) -> dict[str, Any]:
    """Cancel a job before its point of no return (``usage`` after it)."""
    job = service.jobs.cancel(job_id)
    if job.task is not None:
        await asyncio.sleep(0)
    return job.view()
