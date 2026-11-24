"""Who a write is attributed to.

The actor is the human the access token names, never something the request
body says: the body is the client's word, the token is the identity provider's.
The client (Ledger) supplies only what it alone knows -- the producer, i.e. the
model that proposed the candidates, and any assisting agents -- and those are
recorded beside the actor, never instead of it.

pyboltzmann hashes the actor id into every provenance record and refuses ids
that are not an address (``ana@example.com``) or a namespaced name
(``sso.example.com/2f6c...``), lowercase. The token's address claim is used
when it is one; otherwise ``<namespace>/<sub>``.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from boltzmann import Actor, ActorKind
from boltzmann.identity.principal import is_actor_id

from gaussia_brains_mcp_server.auth.principal import Principal
from gaussia_brains_mcp_server.auth.verifier import claim_at
from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.lifecycle.errors import REFUSED_POLICY, LifecycleError

#: The id read-only handles are opened as. Nothing is ever attributed to it.
READER_ACTOR_ID = "gaussia/brains-reader"


def reader_actor() -> Actor:
    return Actor(id=READER_ACTOR_ID, kind=ActorKind.SERVICE, name="read-only")


def _namespace(settings: Settings, principal: Principal) -> str:
    if settings.actor_namespace:
        return settings.actor_namespace.lower()
    # An issuer with no host (the insecure-dev principal's URN) still needs a
    # namespace; "local" says plainly that it names nothing off this machine.
    host = urlsplit(principal.issuer).hostname or "local"
    return host.lower()


def actor_for(principal: Principal, settings: Settings) -> Actor:
    """The actor a write by *principal* is recorded as.

    Raises:
        LifecycleError: ``refused_policy`` when the token yields no usable id.
    """
    claims = principal.claims or {}
    name = claims.get("name") or claims.get("preferred_username")
    address = claim_at(claims, settings.actor_claim)
    if isinstance(address, str) and is_actor_id(address.strip().lower()):
        return Actor(
            id=address.strip().lower(),
            kind=ActorKind.HUMAN,
            name=name if isinstance(name, str) else None,
        )
    candidate = f"{_namespace(settings, principal)}/{principal.subject.lower()}"
    if is_actor_id(candidate):
        return Actor(
            id=candidate,
            kind=ActorKind.HUMAN,
            name=name if isinstance(name, str) else None,
        )
    raise LifecycleError(
        REFUSED_POLICY,
        "the access token names no identity a write can be attributed to: "
        f"claim {settings.actor_claim!r} is not an address and "
        f"{candidate!r} is not a namespaced name",
        detail={"actor_claim": settings.actor_claim},
    )
