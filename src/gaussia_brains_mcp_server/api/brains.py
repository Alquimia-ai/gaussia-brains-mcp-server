"""REST /v1/brains management endpoints."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from gaussia_brains_mcp_server.auth.http import require_scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import CATALOG, PUBLISH
from gaussia_brains_mcp_server.catalog import catalog
from gaussia_brains_mcp_server.config import Settings, get_settings

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


def _mcp_base_url(request: Request, settings: Settings) -> str:
    """Build the absolute MCP path prefix for catalog links."""
    base = str(request.base_url).rstrip("/")
    mcp_path = settings.mcp_path.strip()
    if not mcp_path.startswith("/"):
        mcp_path = "/" + mcp_path
    return f"{base}{mcp_path}"


@router.get("")
async def list_brains(
    request: Request,
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    namespace: Annotated[str | None, Query(description="Namespace index to read")] = None,
    cursor: Annotated[str | None, Query(description="Opaque pagination cursor")] = None,
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum entries to return")] = 20,
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """List brains from namespace indexes with verified signer information."""
    cat = catalog(settings)
    namespaces = [namespace] if namespace else list(settings.effective_catalog_namespaces())
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
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    ref: Annotated[str, Query(description="Brain reference, with tag or digest")],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Legacy query-param detail endpoint; prefer ``GET /v1/brains/{brain_ref}``."""
    cat = catalog(settings)
    expanded = settings.expand_brain_ref(ref)
    return await cat.manifest_detail(expanded)


@router.get("/{brain_ref:path}")
async def get_brain(
    brain_ref: str,
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Return manifest-level detail for a single brain.

    *brain_ref* may be a short reference such as ``brains/my-brain:latest`` or a
    full OCI reference such as ``registry.example.com/acme/handbook:main``.
    """
    cat = catalog(settings)
    expanded = settings.expand_brain_ref(brain_ref)
    return await cat.manifest_detail(expanded)


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
