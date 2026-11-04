"""HTTP enforcement: FastAPI dependencies, RFC 6750 challenges, RFC 9728 metadata."""

from __future__ import annotations

from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from gaussia_brains_mcp_server.auth import scopes as scope_names
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.verifier import InvalidTokenError, TokenVerifier
from gaussia_brains_mcp_server.config import Settings, get_settings

_bearer = HTTPBearer(auto_error=False)

PROTECTED_RESOURCE_WELL_KNOWN = "/.well-known/oauth-protected-resource"


def resource_metadata_url(resource: str) -> str:
    """Where the metadata for *resource* lives (RFC 9728 §3.1).

    The well-known segment goes between host and path:
    ``https://h/brains`` -> ``https://h/.well-known/oauth-protected-resource/brains``.
    """
    parts = urlsplit(resource)
    path = parts.path.rstrip("/")
    return f"{parts.scheme}://{parts.netloc}{PROTECTED_RESOURCE_WELL_KNOWN}{path}"


def metadata_route_path(resource: str) -> str:
    """The local route serving :func:`resource_metadata_url`."""
    return PROTECTED_RESOURCE_WELL_KNOWN + urlsplit(resource).path.rstrip("/")


def protected_resource_metadata(settings: Settings) -> dict[str, Any]:
    """OAuth 2.0 Protected Resource Metadata (RFC 9728 §2).

    MCP clients read this to find which authorization server to get a token
    from and which scopes to ask for.
    """
    metadata: dict[str, Any] = {
        "resource": settings.auth_resource,
        "authorization_servers": [i.issuer for i in settings.auth_issuers],
        "scopes_supported": list(scope_names.ALL),
        "bearer_methods_supported": ["header"],
    }
    if settings.auth_resource_documentation:
        metadata["resource_documentation"] = settings.auth_resource_documentation
    return metadata


def www_authenticate(
    settings: Settings,
    *,
    error: str | None = None,
    description: str | None = None,
    scope: str | None = None,
) -> str:
    """A ``WWW-Authenticate: Bearer`` challenge (RFC 6750 §3, RFC 9728 §5.1)."""
    params = [f'resource_metadata="{resource_metadata_url(settings.auth_resource)}"']
    if error:
        params.append(f'error="{error}"')
    if description:
        params.append(f'error_description="{description.replace(chr(34), chr(39))}"')
    if scope:
        params.append(f'scope="{scope}"')
    return "Bearer " + ", ".join(params)


def _dev_principal() -> Principal:
    return Principal(
        subject="dev-anonymous",
        issuer="urn:gaussia:insecure-dev",
        scopes=frozenset(scope_names.ALL),
    )


def get_token_verifier(request: Request) -> TokenVerifier:
    verifier = getattr(request.app.state, "token_verifier", None)
    if verifier is None:
        raise RuntimeError("app.state.token_verifier is not configured")
    return verifier


async def require_principal(
    request: Request,
    credentials: Annotated[
        HTTPAuthorizationCredentials | None, Security(_bearer)
    ] = None,
) -> Principal:
    """The authenticated caller, or a 401 with a discovery challenge."""
    settings = get_settings()
    if not settings.auth_enabled:
        return _dev_principal()

    if credentials is None or not credentials.credentials:
        # RFC 6750 §3.1: no error code when no credentials were sent.
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": www_authenticate(settings)},
        )
    try:
        principal = await get_token_verifier(request).verify(credentials.credentials)
    except InvalidTokenError as exc:
        logger.info(
            "authn denied route={} reason={} request_id={}",
            request.url.path,
            exc,
            request.headers.get("x-request-id", ""),
        )
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token",
            headers={
                "WWW-Authenticate": www_authenticate(
                    settings, error="invalid_token", description=str(exc)
                )
            },
        ) from exc
    request.state.principal = principal
    return principal


def require_scopes(*required: str):
    """Dependency factory: the caller must hold every scope in *required*."""
    wanted = frozenset(required)

    async def _check(
        principal: Annotated[Principal, Depends(require_principal)],
    ) -> Principal:
        missing = principal.missing_scopes(wanted)
        if missing:
            logger.info(
                "authz denied sub={} iss={} missing_scopes={}",
                principal.subject,
                principal.issuer,
                sorted(missing),
            )
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail=f"Missing scopes: {sorted(missing)}",
                headers={
                    "WWW-Authenticate": www_authenticate(
                        get_settings(),
                        error="insufficient_scope",
                        scope=" ".join(sorted(wanted)),
                    )
                },
            )
        return principal

    return _check


def metadata_router(settings: Settings) -> APIRouter:
    """Router serving the protected resource metadata document (unauthenticated)."""
    router = APIRouter()

    @router.get(metadata_route_path(settings.auth_resource), include_in_schema=False)
    async def _metadata() -> dict[str, Any]:
        return protected_resource_metadata(get_settings())

    return router
