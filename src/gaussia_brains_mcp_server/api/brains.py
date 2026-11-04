"""REST /v1/brains discovery endpoints."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request

from gaussia_brains_mcp_server.auth.http import require_scopes
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.scopes import CATALOG
from gaussia_brains_mcp_server.catalog import catalog
from gaussia_brains_mcp_server.config import Settings, get_settings

router = APIRouter(prefix="/v1/brains", tags=["brains"])


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
    namespaces = [namespace] if namespace else list(settings.catalog_namespaces)
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


@router.get("/manifest")
async def brain_manifest(
    principal: Annotated[Principal, Depends(require_scopes(CATALOG))],
    ref: Annotated[str, Query(description="Brain reference, with tag or digest")],
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> dict[str, Any]:
    """Return manifest-level detail for a single brain."""
    cat = catalog(settings)
    return await cat.manifest_detail(ref)
