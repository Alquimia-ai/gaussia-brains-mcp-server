"""REST /v1/brains management endpoints."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from gaussia_brains_mcp_server.api.lifecycle import (
    ERRORS,
    BrainStatus,
    PublishedBrain,
    error_response,
)
from gaussia_brains_mcp_server.auth.http import (
    insufficient_scope,
    require_any_scope,
    require_scopes,
)
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import ADMIN, CATALOG, PUBLISH, READ, WRITE
from gaussia_brains_mcp_server.catalog import catalog
from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.lifecycle.errors import LifecycleError
from gaussia_brains_mcp_server.lifecycle.identity import actor_for

router = APIRouter(prefix="/v1/brains", tags=["brains"])


class PublishRequest(BaseModel):
    """Body for publishing a brain to the namespace index."""

    description: str = ""
    author: str = ""
    published_at: str = ""


class ReindexRequest(BaseModel):
    """Body for reindexing a namespace or a list of brain references."""

    namespace: str | None = None
    refs: list[str] = Field(default_factory=list)


def _caller_id(principal: Principal, settings: Settings) -> str | None:
    """The caller's actor id, or ``None`` when the token names no usable identity."""
    try:
        return actor_for(principal, settings).id
    except LifecycleError:
        return None


def _mcp_base_url(request: Request, settings: Settings) -> str:
    """Build the absolute MCP path prefix for catalog links."""
    base = str(request.base_url).rstrip("/")
    mcp_path = settings.mcp_path.strip()
    if not mcp_path.startswith("/"):
        mcp_path = "/" + mcp_path
    return f"{base}{mcp_path}"


@router.get("", responses=ERRORS)
async def list_brains(
    request: Request,
    principal: Annotated[Principal, Depends(require_any_scope(CATALOG, WRITE))],
    namespace: Annotated[
        str | None, Query(description="Namespace index to read")
    ] = None,
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[
        int, Query(ge=1, le=100, description="Maximum entries to return")
    ] = 20,
    held: Annotated[
        bool,
        Query(
            description="List the brains held in this service's working store "
            "instead (`brains:catalog` or `brains:write`). Under "
            "GBMS_BRAIN_ACCESS=owners only those the caller may write (every one "
            "with `brains:admin`), by reference; each entry says `writable`."
        ),
    ] = False,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> Any:
    """List brains from namespace indexes with verified signer information.

    Needs ``brains:catalog``; ``held=true`` also accepts ``brains:write``.
    """
    if held:
        service = getattr(request.app.state, "lifecycle", None)
        if service is None:
            return {"entries": [], "next_cursor": None}
        try:
            return await asyncio.to_thread(
                service.list_held,
                actor_id=_caller_id(principal, settings),
                admin=principal.has_scope(ADMIN),
                can_write=principal.has_scope(WRITE),
                cursor=cursor,
                limit=limit,
            )
        except LifecycleError as exc:
            return error_response(request, exc)
    if not principal.has_scope(CATALOG):
        raise insufficient_scope({CATALOG}, f"Missing scopes: {[CATALOG]}")
    cat = catalog(settings)
    namespaces = (
        [namespace] if namespace else list(settings.effective_catalog_namespaces())
    )
    base_url = _mcp_base_url(request, settings)

    all_entries: list[dict[str, Any]] = []
    next_cursor: str | None = None
    for ns in namespaces:
        entries, ns_cursor = await cat.list_entries(
            ns, base_url=base_url, limit=limit - len(all_entries), cursor=cursor
        )
        for entry in entries:
            all_entries.append(
                {
                    "reference": entry.reference,
                    "tag": entry.tag,
                    "digest": entry.digest,
                    "description": entry.description,
                    "published_at": entry.published_at,
                    "claimed_author": entry.claimed_author,
                    "signature": entry.signature,
                    "modules": entry.modules,
                    "mcp": {
                        "pinned": entry.mcp_pinned,
                        "follow": entry.mcp_follow,
                    },
                }
            )
        if ns_cursor:
            next_cursor = ns_cursor
        if len(all_entries) >= limit:
            break

    return {
        "entries": all_entries,
        "next_cursor": next_cursor,
    }


@router.post("/reindex")
async def reindex_brains(
    body: ReindexRequest,
    principal: Annotated[Principal, Depends(require_scopes(PUBLISH))],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Reindex a namespace or a specific list of brain references."""
    cat = catalog(settings)
    try:
        return await cat.reindex(namespace=body.namespace, refs=body.refs or None)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/manifest")
async def brain_manifest_legacy(
    request: Request,
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    ref: Annotated[str, Query(description="Brain reference, with tag or digest")],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Legacy query-param detail endpoint; prefer ``GET /v1/brains/{brain_ref}``."""
    cat = catalog(settings)
    expanded = settings.expand_brain_ref(ref)
    return await cat.manifest_detail(
        expanded, base_url=_mcp_base_url(request, settings)
    )


@router.get("/{brain_ref:path}/tags")
async def brain_tags(
    brain_ref: str,
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Return every tag published for a brain repository."""
    cat = catalog(settings)
    expanded = settings.expand_brain_ref(brain_ref)
    tags = await cat.list_tags(expanded)
    return {"reference": expanded.split("@", 1)[0].rsplit(":", 1)[0], "tags": tags}


@router.get(
    "/{brain_ref:path}",
    responses=ERRORS,
    response_model=BrainStatus | PublishedBrain,
    response_model_exclude_unset=True,
)
async def get_brain(
    request: Request,
    response: Response,
    brain_ref: str,
    principal: Annotated[Principal, Depends(require_any_scope(CATALOG, READ, WRITE))],
    branch: Annotated[
        str | None, Query(description="Branch to report (working-store brains)")
    ] = None,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> Any:
    """Return detail for a single brain.

    A brain held in the working store reports its status: modules and block
    counts, head, branches, any open convergence, and its publication, with
    the branch head as ``ETag`` (send it back as ``If-Match``). Any other
    brain reports its published manifest.

    *brain_ref* may be a short reference such as ``brains/my-brain:latest`` or a
    full OCI reference such as ``registry.example.com/acme/handbook:main``.
    Errors carry the lifecycle's stable ``code``.
    """
    try:
        return await _brain_detail(
            request, response, brain_ref, principal, branch, settings
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - mapped to a stable code
        return error_response(request, exc)


async def _brain_detail(
    request: Request,
    response: Response,
    brain_ref: str,
    principal: Principal,
    branch: str | None,
    settings: Settings,
) -> dict[str, Any]:
    service = getattr(request.app.state, "lifecycle", None)
    if service is not None and "@" not in brain_ref:
        try:
            handle = service.held(brain_ref)
        except Exception:  # noqa: BLE001 - not a working-store reference
            handle = None
        if handle is not None:
            status = await asyncio.to_thread(service.status, handle, branch)
            status["writable"] = principal.has_scope(WRITE) and service.may_write(
                handle.meta,
                _caller_id(principal, settings),
                principal.has_scope(ADMIN),
            )
            if status.get("head"):
                response.headers["ETag"] = f'"{status["head"]}"'
            return status
    if not principal.has_scope(CATALOG):
        raise insufficient_scope({CATALOG}, f"Missing scopes: {[CATALOG]}")
    cat = catalog(settings)
    expanded = settings.expand_brain_ref(brain_ref)
    return await cat.manifest_detail(
        expanded, base_url=_mcp_base_url(request, settings)
    )


@router.post("/{brain_ref:path}/publish")
async def publish_brain(
    brain_ref: str,
    body: PublishRequest,
    principal: Annotated[Principal, Depends(require_scopes(PUBLISH))],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Publish a brain to its namespace index.

    The brain artifact must already exist in the registry; this endpoint only
    creates or refreshes the index entry that makes it discoverable.
    """
    cat = catalog(settings)
    try:
        return await cat.publish(
            brain_ref,
            description=body.description,
            author=body.author,
            published_at=body.published_at,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{brain_ref:path}/unpublish")
async def unpublish_brain(
    brain_ref: str,
    principal: Annotated[Principal, Depends(require_scopes(PUBLISH))],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Unpublish a brain by detaching its index entry."""
    cat = catalog(settings)
    try:
        return await cat.unpublish(brain_ref)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
