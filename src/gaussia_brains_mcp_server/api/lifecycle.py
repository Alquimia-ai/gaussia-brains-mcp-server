"""REST: the brain lifecycle over the working store (``/v1/brains``, ``/v1/jobs``).

Reads (status, browse, history, search) need ``brains:catalog``, ``brains:read``
or ``brains:write``; everything that changes a brain needs ``brains:write`` and
is attributed to the token's human subject. Under ``GBMS_BRAIN_ACCESS=owners``
(the default) a brain is written only by its owner (its creator), the writers
it lists, or a ``brains:admin`` token. Push, pull, converge and import answer
``202`` with a job (``GET /v1/jobs/{id}``); a caller sees and cancels only its
own jobs unless it holds ``brains:admin``.

Errors carry a stable ``code`` (see :mod:`gaussia_brains_mcp_server.lifecycle.errors`)::

    {"code": "not_fast_forward", "message": "...", "detail": {...}}

A request that does not parse is ``usage`` (400) with ``detail.errors``; one
whose body is larger than the service accepts is ``usage`` with 413. Uploads
are read only after the caller is authenticated, and never past the limit.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
from pathlib import Path
from typing import Annotated, Any, Literal

from boltzmann import Actor
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field
from pydantic import ValidationError as PydanticValidationError
from starlette.datastructures import FormData, UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser

from gaussia_brains_mcp_server.auth.http import require_any_scope, require_scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import ADMIN, CATALOG, READ, WRITE
from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.lifecycle.errors import (
    ALL_CODES,
    NOT_FOUND,
    REFUSED_POLICY,
    USAGE,
    LifecycleError,
    describe,
)
from gaussia_brains_mcp_server.lifecycle.identity import actor_for
from gaussia_brains_mcp_server.lifecycle.jobs import Job
from gaussia_brains_mcp_server.lifecycle.service import (
    LifecycleService,
    decode_inline_evidence,
    search_registry,
)


def invalid_request(errors: Sequence[Any]) -> LifecycleError:
    """``usage`` for a request that does not parse, with every error in detail."""
    encoded = jsonable_encoder(list(errors))
    summary = "; ".join(
        f"{'.'.join(str(p) for p in e.get('loc', ()))}: {e.get('msg', '')}"
        for e in encoded[:3]
        if isinstance(e, dict)
    )
    return LifecycleError(
        USAGE,
        f"invalid request: {summary}" if summary else "invalid request",
        detail={"errors": encoded},
    )


def error_response(request: Request, exc: Exception) -> JSONResponse:
    """The stable error body for *exc* (anything unknown is ``internal``)."""
    if isinstance(exc, RequestValidationError):
        exc = invalid_request(exc.errors())
    info = describe(exc)
    if info.code == "internal":
        from loguru import logger

        logger.opt(exception=exc).error(
            "{} {} failed", request.method, request.url.path
        )
    return JSONResponse(status_code=info.status, content=info.body())


class LifecycleRoute(APIRoute):
    """Turns any error raised by a lifecycle handler into its stable error body.

    Authentication and scope failures (:class:`HTTPException`) pass through
    to the application's handler, which renders them in the same contract.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def route(request: Request) -> Response:
            try:
                return await handler(request)
            except HTTPException:
                raise
            except Exception as exc:  # noqa: BLE001 - mapped to a stable code
                return error_response(request, exc)

        return route


router = APIRouter(prefix="/v1/brains", tags=["lifecycle"], route_class=LifecycleRoute)
jobs_router = APIRouter(prefix="/v1/jobs", tags=["jobs"], route_class=LifecycleRoute)

ReadPrincipal = Annotated[Principal, Depends(require_any_scope(CATALOG, READ, WRITE))]
SearchPrincipal = Annotated[Principal, Depends(require_any_scope(READ, CATALOG, WRITE))]
WritePrincipal = Annotated[Principal, Depends(require_scopes(WRITE))]
JobsPrincipal = Annotated[Principal, Depends(require_any_scope(WRITE, ADMIN))]
GrantPrincipal = Annotated[Principal, Depends(require_any_scope(WRITE, ADMIN))]
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
    status: {"model": ErrorBody} for status in (400, 401, 403, 404, 409, 422, 500, 502)
}
UPLOAD_ERRORS: dict[int | str, dict[str, Any]] = {
    **ERRORS,
    413: {"model": ErrorBody, "description": "The body is larger than accepted."},
}

#: Path segments the routes below use after ``{brain_ref:path}``. A brain or
#: branch named with one of them would be shadowed by (or shadow) a route, so
#: they are refused when the brain or branch is created.
RESERVED_SEGMENTS = frozenset(
    {
        "history",
        "blocks",
        "modules",
        "convergence",
        "branches",
        "search",
        "evidence",
        "commits",
        "pull",
        "push",
        "converge",
        "import",
        "resolutions",
        "accept-removals",
        "continue",
        "abort",
        "writers",
        # the discovery routes (api/brains.py)
        "tags",
        "publish",
        "unpublish",
        "manifest",
        "reindex",
    }
)

#: Slack for multipart framing and the small form fields around an upload.
FORM_OVERHEAD_BYTES = 1024 * 1024
#: The largest JSON body the import route reads (it is a handful of strings).
MAX_JSON_BODY_BYTES = 1024 * 1024


class JobView(BaseModel):
    id: str
    kind: Literal["push", "pull", "converge", "import"]
    brain: str
    actor: str
    state: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    stage: str = Field(
        description="What the job is doing; `interrupted` when a restart ended it."
    )
    cancellable: bool
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = Field(
        default=None,
        description="`{code, message, detail}`. Besides the HTTP codes, a job may "
        "end with `cancelled` or `interrupted` (the service restarted while it ran; "
        "`detail.passed_commit_point` says whether it may have changed anything).",
    )


class JobList(BaseModel):
    jobs: list[JobView]


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


class InlineEvidence(BaseModel):
    data_base64: str = Field(description="The source's bytes, base64-encoded.")
    media_type: str = Field(min_length=1)
    filename: str | None = None
    origin: str | None = Field(
        default=None, description="Where the source came from (recorded)."
    )


class CommitRequest(BaseModel):
    source: str | None = Field(
        default=None,
        description="The canonical evidence block the candidates derive from. "
        "Exactly one of `source` and `evidence_inline`.",
    )
    evidence_inline: InlineEvidence | None = Field(
        default=None,
        description="The source's bytes, registered only if the commit goes ahead "
        "(a rejection or dry run writes nothing). Candidates may cite it as "
        "`$source`. Exactly one of `source` and `evidence_inline`.",
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


_SETTLE_BRANCH = (
    "The branch the client believes the convergence is open on; `usage` if it is "
    "open on another."
)


class ResolutionRequest(BaseModel):
    block: str
    kind: Literal["admit", "reject", "prefer"]
    prefer: str | None = None
    reason: str | None = None
    branch: str | None = Field(default=None, description=_SETTLE_BRANCH)


class ReasonRequest(BaseModel):
    reason: str | None = None
    branch: str | None = Field(default=None, description=_SETTLE_BRANCH)


class SettleRequest(BaseModel):
    branch: str | None = Field(default=None, description=_SETTLE_BRANCH)


class WritersRequest(BaseModel):
    writers: list[str] = Field(
        description="Actor ids (an address, or a namespaced name such as "
        "`sso.example.com/<sub>`) who may write besides the owner; replaces the list."
    )


class WritersView(BaseModel):
    reference: str
    owner: str | None = Field(description="The brain's creator, an implicit writer.")
    writers: list[str]
    access: Literal["owners", "open"]


class _Open(BaseModel):
    """A response object that may carry more fields than it documents."""

    model_config = ConfigDict(extra="allow")


class ModuleSummary(_Open):
    block_count: int
    root: str


class BranchView(_Open):
    name: str
    tag: str | None = None
    snapshot: str | None = None
    current: bool = False
    published: str | None = None
    unpublished: bool = False


class RemoteBranchView(_Open):
    name: str
    tag: str
    local: bool


class BranchList(_Open):
    branches: list[BranchView]
    remote: list[RemoteBranchView] | None = Field(
        default=None, description="Present with `remote=true`."
    )


class OpenConvergence(_Open):
    open: bool
    theirs: str | None = Field(
        default=None,
        description="What is being joined, as the client named it when known "
        "(else the snapshot, `theirs_snapshot`).",
    )
    theirs_snapshot: str | None = None
    strategy: str | None = None
    reason: str | None = None
    actor: str | None = Field(default=None, description="Who opened it.")
    started_at: str | None = None
    head: str | None = None
    branch: str | None = Field(
        default=None,
        description="The branch it is open on. It blocks writes to every branch.",
    )
    resolutions: int | None = None


class Publication(_Open):
    reference: str
    tag: str | None = None
    snapshot: str
    up_to_date: bool


class BrainStatus(_Open):
    """A brain held in the working store, at one branch."""

    reference: str
    held: bool
    title: str = ""
    description: str = ""
    default_tag: str
    created_at: str | None = None
    created_by: str | None = None
    imported_from: str | None = None
    owner: str | None = Field(
        default=None, description="Who created it; may always write it."
    )
    writers: list[str] = Field(
        default_factory=list, description="Who else may write it."
    )
    access: Literal["owners", "open"] | None = None
    writable: bool | None = Field(
        default=None, description="Whether the caller may write it."
    )
    governed: bool
    read_only: bool
    branch: str
    head: str | None = None
    current_branch: str | None = None
    modules: dict[str, ModuleSummary] = Field(default_factory=dict)
    block_count: int = 0
    branches: list[BranchView] = Field(default_factory=list)
    convergence: OpenConvergence | None = None
    publication: Publication | None = None
    mcp: dict[str, Any] = Field(default_factory=dict)


class PublishedBrain(_Open):
    """A brain read from the registry: its published manifest."""

    reference: str
    tag: str | None = None
    digest: str | None = None
    modules: list[str] = Field(default_factory=list)
    annotations: dict[str, Any] | None = None
    signature: dict[str, Any] | None = None
    mcp: dict[str, Any] | None = None


class BlockRow(_Open):
    block_id: str
    memory_type: str
    added_at: str | None = None
    superseded_by: str | None = None
    demoted: bool = False
    tombstoned: bool = False
    readable: bool = True
    unreadable_reason: str | None = Field(
        default=None, description="Present when the block could not be read."
    )
    summary: str | None = None
    payload: Any = None


class BlockPage(_Open):
    memory_type: str
    snapshot: str
    total: int
    blocks: list[BlockRow]
    next_cursor: str | None = None


class BlockDetail(BlockRow):
    snapshot: str
    root: str
    verifies: dict[str, bool]
    text: Any = None
    resolvable: bool | None = None
    provenance: list[Any] = Field(default_factory=list)
    supersedes: list[str] = Field(default_factory=list)
    competing_successors: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    dependents: list[str] = Field(default_factory=list)
    producer: Any = None
    assisted_by: list[Any] = Field(default_factory=list)
    validation: Any = None
    locator: Any = None
    removed: bool = False


class SnapshotRow(_Open):
    snapshot: str
    pruned: bool | None = Field(
        default=None, description="The snapshot is no longer held; the chain ends."
    )
    created_at: str | None = None
    parents: list[str] | None = None
    reconciliation: bool | None = None
    labels: dict[str, Any] | None = None
    governed: bool | None = None
    modules: dict[str, ModuleSummary] | None = None
    published: bool | None = None


class HistoryPage(_Open):
    branch: str
    head: str | None = None
    snapshots: list[SnapshotRow]
    next_cursor: str | None = None


class SearchSource(_Open):
    block_id: str
    locator: Any = None


class SearchMatch(_Open):
    block_id: str
    memory_type: str
    content: Any = None
    score: float | None = None
    sources: list[SearchSource] = Field(default_factory=list)
    locator: Any = None
    resolvable: bool | None = None
    verified: bool | None = None


class SearchNarrowing(_Open):
    requested_memory_types: list[str] | None = None
    searched_memory_types: list[str] = Field(default_factory=list)
    missing_memory_types: list[str] = Field(default_factory=list)
    semantic: Any = None
    semantic_reason: str | None = Field(
        default=None,
        description=(
            "Why vector search did not score the query (null when it did): "
            "not_routed, no_vector_index, no_vectors, model_mismatch, "
            "embedding_failed, not_text, unavailable."
        ),
    )
    indices_used: list[str] | None = Field(
        default=None, description="Index kinds that returned candidates."
    )
    dropped_unverified: int = 0


class SearchResponse(_Open):
    results: list[SearchMatch]
    truncated: bool
    verified_against: dict[str, str] = Field(default_factory=dict)
    narrowing: SearchNarrowing
    snapshot: str | None = Field(default=None, description="Working-store brains.")
    branch: str | None = Field(default=None, description="Working-store brains.")
    reference: str | None = Field(default=None, description="Registry brains.")


class CandidateVerdict(_Open):
    index: int
    status: str = Field(description="`accepted`, `rejected`, ...")
    memory_type: str
    block_id: str | None = None
    issues: list[Any] = Field(default_factory=list)
    conflicts_with: list[str] = Field(default_factory=list)
    candidate: Any = None


class ValidationReport(_Open):
    clean: bool
    committable: int
    results: list[CandidateVerdict]
    producer: Any = None
    task_id: str | None = None
    checks: list[Any] = Field(default_factory=list)


class CommittedBlock(_Open):
    block_id: str
    memory_type: str
    payload: Any = None


class CommitResponse(_Open):
    """A dry run (``report`` with every verdict, rejected ones included) or a commit."""

    dry_run: bool
    report: ValidationReport
    head: str | None = None
    committed: list[CommittedBlock] | None = None
    provenance: list[str] | None = None
    snapshot: str | None = None
    roots: dict[str, str] | None = None
    branch: str | None = None
    source: str | None = Field(
        default=None,
        description="With `evidence_inline`: the canonical block it is (or would be).",
    )
    evidence_duplicate: bool | None = Field(
        default=None,
        description="With `evidence_inline`: the brain already held those bytes.",
    )


class ConvergenceView(_Open):
    open: bool
    branch: str | None = None
    actor: str | None = None
    theirs: str | None = None
    strategy: str | None = None
    reason: str | None = None
    started_at: str | None = None
    resolved: bool | None = None
    unresolved: list[str] | None = None
    removals_accepted: bool | None = None
    status: Any = None


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
            detail={"replica": "reader"},
        )
    return service


Service = Annotated[LifecycleService, Depends(get_service)]


def who(principal: Principal, settings: Settings) -> tuple[Actor, bool]:
    """The actor a write is attributed to, and whether the caller is an admin."""
    return actor_for(principal, settings), principal.has_scope(ADMIN)


def _job_response(job: Job) -> JSONResponse:
    return JSONResponse(
        status_code=202, content=job.view(), headers={"Location": f"/v1/jobs/{job.id}"}
    )


def check_brain_name(reference: str) -> None:
    """Refuse a new brain whose path would collide with a route (``usage``)."""
    path = reference.split("@", 1)[0]
    segments = path.split("/")
    segments[-1] = segments[-1].split(":", 1)[0]
    clashes = sorted({seg for seg in segments if seg.lower() in RESERVED_SEGMENTS})
    if clashes:
        raise LifecycleError(
            USAGE,
            f"a brain cannot be named with the segment(s) {clashes}: they are "
            f"reserved by the REST routes",
            detail={"reserved": clashes},
        )


def check_branch_name(name: str) -> None:
    """Refuse a branch name the branch routes could not address (``usage``)."""
    segments = name.split("/")
    if "branches" in segments or segments[-1] == "push":
        raise LifecycleError(
            USAGE,
            "a branch name cannot contain a `branches` segment or end in `push`: "
            "the branch routes could not address it",
        )


def _too_large(limit: int) -> LifecycleError:
    return LifecycleError(
        USAGE,
        f"the request body is larger than {limit} bytes",
        detail={"limit": limit},
        status=413,
    )


async def _limited_stream(request: Request, limit: int) -> AsyncGenerator[bytes, None]:
    """The request body, refused (413) as soon as it passes *limit* bytes."""
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        raise _too_large(limit)
    seen = 0
    async for chunk in request.stream():
        seen += len(chunk)
        if seen > limit:
            raise _too_large(limit)
        yield chunk


async def _read_form(request: Request, limit: int) -> FormData:
    """Parse a multipart body, reading at most *limit* bytes of it."""
    content_type = request.headers.get("content-type", "")
    if not content_type.startswith("multipart/form-data"):
        raise LifecycleError(USAGE, "the body must be multipart/form-data")
    parser = MultiPartParser(
        request.headers, _limited_stream(request, limit), max_files=4, max_fields=16
    )
    try:
        return await parser.parse()
    except MultiPartException as exc:
        raise LifecycleError(USAGE, f"invalid multipart body: {exc.message}") from exc


async def _read_body(request: Request, limit: int) -> bytes:
    return b"".join([chunk async for chunk in _limited_stream(request, limit)])


# --------------------------------------------------------------------------- #
# Create, status, browse
# --------------------------------------------------------------------------- #


@router.post(
    "",
    status_code=201,
    responses=ERRORS,
    response_model=BrainStatus,
    response_model_exclude_unset=True,
)
async def create_brain(
    body: CreateBrainRequest,
    principal: WritePrincipal,
    service: Service,
    response: Response,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Create an empty, ungoverned brain in the working store.

    Its first evidence or commit creates its history. A reference with a path
    segment the routes use (``history``, ``blocks``, ``branches``, ...) is
    refused with ``usage``.
    """
    check_brain_name(body.reference)
    status = service.create(
        body.reference,
        actor=actor_for(principal, settings),
        default_tag=body.default_tag,
        title=body.title,
        description=body.description,
    )
    status["writable"] = True
    response.headers["Location"] = f"/v1/brains/{status['reference']}"
    return status


@router.get(
    "/{brain_ref:path}/modules/{memory_type}/blocks",
    responses=ERRORS,
    response_model=BlockPage,
    response_model_exclude_unset=True,
)
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


@router.get(
    "/{brain_ref:path}/blocks/{block_id}",
    responses=ERRORS,
    response_model=BlockDetail,
    response_model_exclude_unset=True,
)
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


@router.get(
    "/{brain_ref:path}/history",
    responses=ERRORS,
    response_model=HistoryPage,
    response_model_exclude_unset=True,
)
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


@router.post(
    "/{brain_ref:path}/search",
    responses=ERRORS,
    response_model=SearchResponse,
    response_model_exclude_unset=True,
)
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


_EVIDENCE_FORM = {
    "type": "object",
    "required": ["file"],
    "properties": {
        "file": {
            "type": "string",
            "format": "binary",
            "description": "The source, byte for byte.",
        },
        "media_type": {"type": "string"},
        "origin": {"type": "string"},
        "license": {"type": "string"},
        "branch": {"type": "string"},
    },
}


@router.post(
    "/{brain_ref:path}/evidence",
    status_code=201,
    responses=UPLOAD_ERRORS,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"multipart/form-data": {"schema": _EVIDENCE_FORM}},
        }
    },
)
async def register_evidence(
    brain_ref: str,
    request: Request,
    principal: WritePrincipal,
    service: Service,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Register a source as canonical evidence (multipart). Identical bytes are a no-op.

    A source larger than ``GBMS_STORE_MAX_EVIDENCE_BYTES`` is ``usage`` (413).
    """
    # The form is read here, after the caller is authenticated, and never past
    # the limit; declaring it as File/Form parameters would spool it first.
    limit = settings.store_max_evidence_bytes
    form = await _read_form(request, limit + FORM_OVERHEAD_BYTES)
    try:
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise LifecycleError(USAGE, "the form needs a `file` part")
        data = await file.read(limit + 1)
        if len(data) > limit:
            raise _too_large(limit)
        handle = service.handle(brain_ref)
        return await service.register_evidence(
            handle,
            data,
            actor=actor_for(principal, settings),
            admin=principal.has_scope(ADMIN),
            media_type=_form_str(form.get("media_type"))
            or file.content_type
            or "application/octet-stream",
            origin=_form_str(form.get("origin")),
            license=_form_str(form.get("license")),
            branch=_form_str(form.get("branch")),
            if_match=if_match,
        )
    finally:
        await form.close()


@router.post(
    "/{brain_ref:path}/commits",
    responses=UPLOAD_ERRORS,
    response_model=CommitResponse,
    response_model_exclude_unset=True,
)
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

    The source is either ``source`` (evidence already registered) or
    ``evidence_inline`` (its bytes, base64): then validation runs against the
    brain as it would be with the evidence added, and the evidence is
    registered only if the commit goes ahead, under the same lock and the same
    ``If-Match`` check -- a rejection or dry run leaves the brain untouched.
    Candidates cite inline evidence as ``$source``; the response names it
    (``source``, ``evidence_duplicate``). Inline evidence larger than
    ``GBMS_STORE_MAX_EVIDENCE_BYTES`` (decoded) is ``usage`` (413).
    """
    if (body.source is None) == (body.evidence_inline is None):
        raise LifecycleError(
            USAGE, "give exactly one of `source` and `evidence_inline`"
        )
    evidence: bytes | None = None
    inline = body.evidence_inline
    if inline is not None:
        evidence = decode_inline_evidence(
            inline.data_base64, settings.store_max_evidence_bytes
        )
    handle = service.handle(brain_ref)
    result = await service.commit(
        handle,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        source=body.source,
        evidence=evidence,
        evidence_media_type=inline.media_type if inline else None,
        evidence_origin=(inline.origin or inline.filename) if inline else None,
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
# Who may write
# --------------------------------------------------------------------------- #


@router.get("/{brain_ref:path}/writers", response_model=WritersView, responses=ERRORS)
async def get_writers(
    brain_ref: str, principal: ReadPrincipal, service: Service
) -> dict[str, Any]:
    """Who may write the brain: its owner (creator) and its writers.

    ``access`` is ``GBMS_BRAIN_ACCESS``: under ``open`` any ``brains:write``
    holder may write any brain and the list is not enforced.
    """
    return service.writers(service.handle(brain_ref))


@router.put("/{brain_ref:path}/writers", response_model=WritersView, responses=ERRORS)
async def put_writers(
    brain_ref: str,
    body: WritersRequest,
    principal: GrantPrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Replace who besides the owner may write the brain.

    Only the brain's owner or a ``brains:admin`` caller may (``refused_policy``
    otherwise); an id that is not an address or a namespaced name is ``usage``.
    """
    handle = service.handle(brain_ref)
    actor, admin = who(principal, settings)
    return await asyncio.to_thread(
        service.set_writers, handle, body.writers, actor=actor, admin=admin
    )


# --------------------------------------------------------------------------- #
# Branches and distribution
# --------------------------------------------------------------------------- #


@router.get(
    "/{brain_ref:path}/branches",
    responses=ERRORS,
    response_model=BranchList,
    response_model_exclude_unset=True,
)
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
    if_match: IfMatch = None,
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
        admin=principal.has_scope(ADMIN),
        force=bool(body and body.force),
        if_match=if_match,
    )
    return _job_response(job)


@router.post("/{brain_ref:path}/branches", status_code=201, responses=ERRORS)
async def create_branch(
    brain_ref: str,
    body: CreateBranchRequest,
    principal: WritePrincipal,
    service: Service,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Create a branch; a name with a `branches` segment or ending in `push` is ``usage``."""
    check_branch_name(body.name)
    handle = service.handle(brain_ref)
    return await service.create_branch(
        handle,
        body.name,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        start=body.start,
        if_match=if_match,
    )


@router.delete("/{brain_ref:path}/branches/{name:path}", responses=ERRORS)
async def delete_branch(
    brain_ref: str,
    name: str,
    principal: WritePrincipal,
    service: Service,
    force: Annotated[bool, Query()] = False,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Delete a branch's local ref; its registry tag stays."""
    handle = service.handle(brain_ref)
    return await service.delete_branch(
        handle,
        name,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        force=force,
        if_match=if_match,
    )


@router.post("/{brain_ref:path}/pull", status_code=202, responses=ERRORS)
async def pull(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: PullRequest | None = None,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> JSONResponse:
    handle = service.handle(brain_ref)
    job = service.pull(
        handle,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        tag=body.tag if body else None,
        branch=body.branch if body else None,
        if_match=if_match,
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
    if_match: IfMatch = None,
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
        admin=principal.has_scope(ADMIN),
        source=body.from_,
        into=body.into,
        strategy=body.strategy,
        reason=body.reason,
        fast_forward=body.fast_forward,
        if_match=if_match,
    )
    return _job_response(job)


@router.get(
    "/{brain_ref:path}/convergence",
    responses=ERRORS,
    response_model=ConvergenceView,
    response_model_exclude_unset=True,
)
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
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Answer one open question: admit, reject, or prefer a successor."""
    handle = service.handle(brain_ref)
    return await service.resolve(
        handle,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        block=body.block,
        kind=body.kind,
        prefer=body.prefer,
        reason=body.reason,
        branch=body.branch,
        if_match=if_match,
    )


@router.post("/{brain_ref:path}/convergence/accept-removals", responses=ERRORS)
async def accept_removals(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: ReasonRequest | None = None,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    handle = service.handle(brain_ref)
    return await service.accept_removals(
        handle,
        actor=actor_for(principal, settings),
        admin=principal.has_scope(ADMIN),
        reason=body.reason if body else None,
        branch=body.branch if body else None,
        if_match=if_match,
    )


@router.post("/{brain_ref:path}/convergence/continue", responses=ERRORS)
async def continue_convergence(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: SettleRequest | None = None,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Conclude a convergence whose questions are answered (``needs_human`` if not).

    Only whoever opened it, the brain's owner or a ``brains:admin`` caller may
    settle a convergence (``GBMS_BRAIN_ACCESS=owners``).
    """
    handle = service.handle(brain_ref)
    actor, admin = who(principal, settings)
    return await service.continue_convergence(
        handle,
        actor=actor,
        admin=admin,
        branch=body.branch if body else None,
        if_match=if_match,
    )


@router.post("/{brain_ref:path}/convergence/abort", responses=ERRORS)
async def abort_convergence(
    brain_ref: str,
    principal: WritePrincipal,
    service: Service,
    body: SettleRequest | None = None,
    if_match: IfMatch = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Abandon the open convergence; the brain is left as it was before it."""
    handle = service.handle(brain_ref)
    actor, admin = who(principal, settings)
    return await service.abort_convergence(
        handle,
        actor=actor,
        admin=admin,
        branch=body.branch if body else None,
        if_match=if_match,
    )


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #


@router.post(
    "/{brain_ref:path}/import",
    status_code=202,
    responses=UPLOAD_ERRORS,
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
    are and served read-only. An upload larger than ``GBMS_STORE_MAX_IMPORT_BYTES``
    is ``usage`` (413); a new brain named with a reserved segment is ``usage``.
    """
    actor = actor_for(principal, settings)
    if service.held(brain_ref) is None:
        check_brain_name(brain_ref)
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("multipart/form-data"):
        limit = settings.store_max_import_bytes
        form = await _read_form(request, limit + FORM_OVERHEAD_BYTES)
        try:
            upload = form.get("layout")
            if not isinstance(upload, UploadFile):
                raise LifecycleError(USAGE, "the form needs a `layout` file")
            archive = await _spool(upload, settings, service)
            job = service.import_layout(
                brain_ref,
                archive,
                actor=actor,
                admin=principal.has_scope(ADMIN),
                default_tag=_form_str(form.get("default_tag")),
                title=_form_str(form.get("title")) or "",
                description=_form_str(form.get("description")) or "",
            )
        finally:
            await form.close()
        return _job_response(job)
    raw = await _read_body(request, MAX_JSON_BODY_BYTES)
    try:
        body = ImportRequest.model_validate_json(raw or b"{}")
    except PydanticValidationError as exc:
        raise invalid_request(exc.errors()) from exc
    job = service.import_from_registry(
        brain_ref,
        actor=actor,
        admin=principal.has_scope(ADMIN),
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
                    raise _too_large(limit)
                out.write(chunk)
    except BaseException:
        Path(path).unlink(missing_ok=True)
        raise
    return Path(path)


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #


def _visible_job(
    service: LifecycleService, job_id: str, principal: Principal, settings: Settings
) -> Job:
    """The job if the caller may see it: its own, or any with ``brains:admin``.

    Someone else's job is ``not_found``, as if it did not exist.
    """
    job = service.jobs.get(job_id)
    if principal.has_scope(ADMIN) or job.actor == actor_for(principal, settings).id:
        return job
    raise LifecycleError(NOT_FOUND, f"no job {job_id!r}")


@jobs_router.get("", response_model=JobList, responses=ERRORS)
async def list_jobs(
    principal: JobsPrincipal,
    service: Service,
    brain: Annotated[str | None, Query(description="Only jobs on this brain")] = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """The caller's jobs, newest first (every job with ``brains:admin``)."""
    reference = service.repository(brain)[0] if brain else None
    actor = None if principal.has_scope(ADMIN) else actor_for(principal, settings).id
    jobs = service.jobs.list(brain=reference, actor=actor)
    return {"jobs": [job.view() for job in jobs]}


@jobs_router.get("/{job_id}", response_model=JobView, responses=ERRORS)
async def get_job(
    job_id: str,
    principal: JobsPrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """One of the caller's jobs (any job with ``brains:admin``)."""
    return _visible_job(service, job_id, principal, settings).view()


@jobs_router.post("/{job_id}/cancel", response_model=JobView, responses=ERRORS)
async def cancel_job(
    job_id: str,
    principal: JobsPrincipal,
    service: Service,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Cancel a job before its point of no return (``usage`` after it)."""
    _visible_job(service, job_id, principal, settings)
    job = service.jobs.cancel(job_id)
    if job.task is not None:
        await asyncio.sleep(0)
    return job.view()
