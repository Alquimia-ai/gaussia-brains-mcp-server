"""Adapter plugging :class:`TokenVerifier` into the MCP SDK's bearer auth.

The MCP authorization spec makes an HTTP MCP server an OAuth 2.1 resource
server; the SDK's middleware calls ``verify_token`` and expects an
``AccessToken``. The :class:`Principal` rides along in ``claims`` so tool
handlers authorize on exactly what REST handlers authorize on.
"""

from __future__ import annotations

from mcp.server.auth.provider import AccessToken

from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.verifier import InvalidTokenError, TokenVerifier

PRINCIPAL_CLAIM = "x-gbms-principal"


class McpTokenVerifier:
    """``mcp.server.auth.provider.TokenVerifier`` backed by :class:`TokenVerifier`."""

    def __init__(self, verifier: TokenVerifier) -> None:
        self._verifier = verifier

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            principal = await self._verifier.verify(token)
        except InvalidTokenError:
            return None
        aud = principal.claims.get("aud")
        resource: str | None = self._verifier.settings.auth_resource
        if isinstance(aud, list):
            resource = resource if resource in aud else (aud[0] if aud else None)
        elif isinstance(aud, str):
            resource = aud
        return AccessToken(
            token=token,
            client_id=principal.client_id or principal.subject,
            scopes=sorted(principal.scopes),
            expires_at=principal.claims.get("exp"),
            resource=resource,
            subject=principal.subject,
            claims={**principal.claims, PRINCIPAL_CLAIM: principal},
        )


def principal_from_access_token(token: AccessToken) -> Principal:
    """Recover the :class:`Principal` an :class:`McpTokenVerifier` attached."""
    principal = (token.claims or {}).get(PRINCIPAL_CLAIM)
    if not isinstance(principal, Principal):
        raise LookupError("access token was not produced by McpTokenVerifier")
    return principal
