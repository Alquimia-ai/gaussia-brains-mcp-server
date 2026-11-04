"""JWT access-token validation for an OAuth 2.0 resource server.

Standards followed:

* RFC 7519 (JWT) / RFC 7515 (JWS): signature, ``exp``/``nbf``/``iat``.
* RFC 9068 (JWT profile for access tokens): ``iss``, ``aud``, optional ``typ=at+jwt``.
* RFC 8707 (resource indicators): the audience is this service's resource id.
* RFC 8414 / OIDC Discovery + RFC 7517: keys come from the issuer's JWKS.
* RFC 8693 (token exchange): ``act`` is surfaced as the acting party.

Any issuer speaking these works -- Keycloak, Entra ID, Auth0, Dex, Kubernetes
service-account token issuers, or a SPIRE OIDC Discovery Provider (JWT-SVIDs
are plain JWTs) -- with no provider-specific code or SDK.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import httpx
import jwt
from loguru import logger

from gaussia_brains_mcp_server.auth.jwks import IssuerKeys, KeyResolutionError
from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.config import Settings, TrustedIssuer


class InvalidTokenError(Exception):
    """The bearer token is not acceptable (RFC 6750 ``invalid_token``)."""


def _normalize_typ(value: Any) -> str:
    # RFC 9068 §2.1: "at+jwt"; "application/at+jwt" is the same media type.
    text = str(value or "").lower()
    return text.removeprefix("application/")


def claim_at(payload: dict[str, Any], path: str) -> Any:
    """Look up a dotted *path* (``realm_access.roles``) in *payload*."""
    if path in payload:
        return payload[path]
    node: Any = payload
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _strings(value: Any, *, split_spaces: bool = False) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return value.split() if split_spaces else [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if isinstance(item, (str, int))]
    return []


def _collect(
    payload: dict[str, Any], paths: Iterable[str], *, split_spaces: bool = False
) -> set[str]:
    found: set[str] = set()
    for path in paths:
        found.update(_strings(claim_at(payload, path), split_spaces=split_spaces))
    return found


class TokenVerifier:
    """Validates bearer tokens against the configured trusted issuers."""

    def __init__(
        self, settings: Settings, http: httpx.AsyncClient | None = None
    ) -> None:
        self.settings = settings
        self._owns_http = http is None
        self._http = http or httpx.AsyncClient(
            timeout=settings.auth_http_timeout_seconds, follow_redirects=False
        )
        self._issuers: dict[str, TrustedIssuer] = {
            i.issuer: i for i in settings.auth_issuers
        }
        self._keys: dict[str, IssuerKeys] = {
            i.issuer: IssuerKeys(
                i,
                self._http,
                ttl=settings.auth_jwks_cache_ttl_seconds,
                min_refresh=settings.auth_jwks_min_refresh_seconds,
            )
            for i in settings.auth_issuers
        }

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def verify(self, token: str) -> Principal:
        """Validate *token* and return the caller. Raises :class:`InvalidTokenError`."""
        try:
            header = jwt.get_unverified_header(token)
            unverified = jwt.decode(token, options={"verify_signature": False})
        except jwt.PyJWTError as exc:
            raise InvalidTokenError("malformed token") from exc

        # The issuer is read before the signature is checked only to choose
        # which keys to check it with; it is validated again by `jwt.decode`.
        issuer_id = unverified.get("iss")
        issuer = self._issuers.get(issuer_id) if isinstance(issuer_id, str) else None
        if issuer is None:
            raise InvalidTokenError("token issuer is not trusted")

        alg = header.get("alg")
        if alg not in issuer.algorithms:
            raise InvalidTokenError(f"signing algorithm {alg!r} is not accepted")
        if issuer.required_typ and _normalize_typ(header.get("typ")) != _normalize_typ(
            issuer.required_typ
        ):
            raise InvalidTokenError(f"token type must be {issuer.required_typ!r}")

        try:
            key = await self._keys[issuer.issuer].get_key(header.get("kid"))
        except KeyResolutionError as exc:
            logger.warning("Token key resolution failed: {}", exc)
            raise InvalidTokenError("token signing key is unknown") from exc
        # A JWK that pins its algorithm must not be used with another one.
        if key.alg is not None and key.alg != alg:
            raise InvalidTokenError("token algorithm does not match its signing key")

        audiences = issuer.audiences or [self.settings.auth_resource]
        try:
            payload = jwt.decode(
                token,
                key.key,
                algorithms=[alg],
                audience=audiences,
                issuer=issuer.issuer,
                leeway=self.settings.auth_leeway_seconds,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise InvalidTokenError("token has expired") from exc
        except jwt.InvalidAudienceError as exc:
            raise InvalidTokenError("token was not issued for this resource") from exc
        except jwt.PyJWTError as exc:
            raise InvalidTokenError(
                f"token validation failed: {type(exc).__name__}"
            ) from exc

        return self._principal(payload, issuer)

    def _principal(self, payload: dict[str, Any], issuer: TrustedIssuer) -> Principal:
        mapping = issuer.claims
        subject = claim_at(payload, mapping.subject)
        if not isinstance(subject, str) or not subject:
            raise InvalidTokenError(f"token has no subject claim {mapping.subject!r}")

        roles = _collect(payload, mapping.roles)
        scopes = _collect(payload, mapping.scopes, split_spaces=True)
        for role in roles:
            scopes.update(issuer.role_scopes.get(role, ()))

        client_id = next(iter(sorted(_collect(payload, mapping.client_id))), None)
        act = payload.get("act")
        actor = act.get("sub") if isinstance(act, dict) else None

        return Principal(
            subject=subject,
            issuer=issuer.issuer,
            client_id=client_id,
            scopes=frozenset(scopes),
            roles=frozenset(roles),
            actor=actor if isinstance(actor, str) else None,
            claims=payload,
        )
