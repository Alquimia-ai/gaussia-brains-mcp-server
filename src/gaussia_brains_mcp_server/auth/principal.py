"""The normalized identity of a caller, built from a validated access token."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Principal:
    """Who is calling, and what the token says they may do.

    Nothing here is specific to an identity provider or to any one platform.

    Attributes:
        subject: The token's subject (``sub`` or the mapped claim). Unique per issuer.
        issuer: The validated ``iss``.
        client_id: The OAuth client the token was issued to (``client_id``/``azp``).
        scopes: OAuth scopes, plus any granted through the issuer's ``role_scopes``.
        roles: Roles as the issuer states them; informational unless mapped to scopes.
        actor: The acting party for delegated tokens (RFC 8693 ``act.sub``).
        claims: The full validated payload, for auditing.
    """

    subject: str
    issuer: str
    client_id: str | None = None
    scopes: frozenset[str] = field(default_factory=frozenset)
    roles: frozenset[str] = field(default_factory=frozenset)
    actor: str | None = None
    claims: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    def has_scope(self, scope: str) -> bool:
        return scope in self.scopes

    def has_all_scopes(self, scopes: set[str] | frozenset[str]) -> bool:
        return set(scopes) <= self.scopes

    def missing_scopes(self, scopes: set[str] | frozenset[str]) -> set[str]:
        return set(scopes) - self.scopes

    def audit(self) -> dict[str, Any]:
        """Fields safe to put in an audit log line."""
        return {
            "sub": self.subject,
            "iss": self.issuer,
            "client_id": self.client_id,
            "act": self.actor,
        }
