from __future__ import annotations

import time
from typing import Annotated

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from conftest import ISSUER, RESOURCE, FakeIdp
from gaussia_brains_mcp_server.auth import Principal, scopes
from gaussia_brains_mcp_server.auth.http import (
    metadata_route_path,
    metadata_router,
    require_scopes,
    resource_metadata_url,
)
from gaussia_brains_mcp_server.auth.jwks import discovery_urls
from gaussia_brains_mcp_server.auth.mcp import (
    McpTokenVerifier,
    principal_from_access_token,
)
from gaussia_brains_mcp_server.auth.verifier import InvalidTokenError, TokenVerifier
from gaussia_brains_mcp_server.config import Settings, TrustedIssuer


def make_settings(**issuer_overrides) -> Settings:
    issuer = {
        "issuer": ISSUER,
        "claims": {
            "roles": ["realm_access.roles"],
        },
        "role_scopes": {"brain-reader": [scopes.READ]},
        **issuer_overrides,
    }
    return Settings(
        auth_resource=RESOURCE,
        auth_issuers=[TrustedIssuer(**issuer)],
        auth_jwks_min_refresh_seconds=0,
    )


def verifier_for(idp: FakeIdp, **issuer_overrides) -> TokenVerifier:
    return TokenVerifier(make_settings(**issuer_overrides), http=idp.client())


# -- happy path & claim mapping ---------------------------------------------- #


async def test_valid_token_maps_claims_to_principal(idp: FakeIdp) -> None:
    token = idp.token(
        scope="brains:catalog openid",
        realm_access={"roles": ["brain-reader"]},
        azp="runtime",
        act={"sub": "agent:summarizer"},
    )
    principal = await verifier_for(idp).verify(token)

    assert principal.subject == "agent-1"
    assert principal.issuer == ISSUER
    assert principal.client_id == "runtime"
    assert principal.scopes == {"brains:catalog", "openid", scopes.READ}
    assert principal.roles == {"brain-reader"}
    assert principal.actor == "agent:summarizer"


async def test_scp_list_claim_is_accepted(idp: FakeIdp) -> None:
    principal = await verifier_for(idp).verify(idp.token(scp=[scopes.READ]))
    assert principal.has_scope(scopes.READ)


async def test_ec_keys_work(idp: FakeIdp) -> None:
    idp.add_ec("e1")
    principal = await verifier_for(idp).verify(idp.token("e1"))
    assert principal.subject == "agent-1"


# -- rejections --------------------------------------------------------------- #


async def test_wrong_audience_is_rejected(idp: FakeIdp) -> None:
    with pytest.raises(InvalidTokenError, match="not issued for this resource"):
        await verifier_for(idp).verify(idp.token(aud="https://other.example.com"))


async def test_explicit_audiences_override_resource(idp: FakeIdp) -> None:
    v = verifier_for(idp, audiences=["gaussia-brains"])
    assert (await v.verify(idp.token(aud="gaussia-brains"))).subject == "agent-1"
    with pytest.raises(InvalidTokenError):
        await v.verify(idp.token())


async def test_expired_token_is_rejected(idp: FakeIdp) -> None:
    past = int(time.time()) - 3600
    with pytest.raises(InvalidTokenError, match="expired"):
        await verifier_for(idp).verify(idp.token(iat=past, exp=past + 60))


async def test_missing_exp_is_rejected(idp: FakeIdp) -> None:
    with pytest.raises(InvalidTokenError):
        await verifier_for(idp).verify(idp.token(exp=None))


async def test_untrusted_issuer_is_rejected(idp: FakeIdp) -> None:
    with pytest.raises(InvalidTokenError, match="issuer is not trusted"):
        await verifier_for(idp).verify(idp.token(iss="https://evil.example.com"))
    # Nothing was fetched for an issuer that is not configured.
    assert idp.requests == []


async def test_hs256_token_is_rejected_even_with_public_key_as_secret(
    idp: FakeIdp,
) -> None:
    # Classic algorithm-confusion attempt: HMAC "signed" with the public key.
    forged = jwt.encode(
        {"iss": ISSUER, "sub": "x", "aud": RESOURCE, "exp": int(time.time()) + 60},
        "public-key-bytes-as-secret-0123456789abcdef",
        algorithm="HS256",
        headers={"kid": "k1"},
    )
    with pytest.raises(InvalidTokenError, match="algorithm"):
        await verifier_for(idp).verify(forged)


async def test_unsigned_token_is_rejected(idp: FakeIdp) -> None:
    unsigned = jwt.encode(
        {"iss": ISSUER, "sub": "x", "aud": RESOURCE, "exp": int(time.time()) + 60},
        None,  # type: ignore[arg-type]
        algorithm="none",
    )
    with pytest.raises(InvalidTokenError):
        await verifier_for(idp).verify(unsigned)


async def test_key_pinned_alg_must_match_header(idp: FakeIdp) -> None:
    token = idp.token(alg="PS256")  # k1's JWK pins RS256
    with pytest.raises(InvalidTokenError, match="does not match"):
        await verifier_for(idp).verify(token)


async def test_tampered_signature_is_rejected(idp: FakeIdp) -> None:
    head, body, sig = idp.token().split(".")
    tampered = f"{head}.{body}.{sig[:-4]}AAAA"
    with pytest.raises(InvalidTokenError):
        await verifier_for(idp).verify(tampered)


async def test_required_typ(idp: FakeIdp) -> None:
    v = verifier_for(idp, required_typ="at+jwt")
    with pytest.raises(InvalidTokenError, match="token type"):
        await v.verify(idp.token())
    ok = await v.verify(idp.token(headers={"typ": "application/at+jwt"}))
    assert ok.subject == "agent-1"


async def test_missing_subject_is_rejected(idp: FakeIdp) -> None:
    with pytest.raises(InvalidTokenError, match="subject"):
        await verifier_for(idp).verify(idp.token(sub=None))


# -- keys & discovery --------------------------------------------------------- #


async def test_key_rotation_refetches_jwks_on_unknown_kid(idp: FakeIdp) -> None:
    v = verifier_for(idp)
    await v.verify(idp.token("k1"))
    idp.add_rsa("k2")
    principal = await v.verify(idp.token("k2"))
    assert principal.subject == "agent-1"
    assert idp.requests.count("https://idp.example.com/jwks") == 2


async def test_unknown_kid_refetch_is_rate_limited(idp: FakeIdp) -> None:
    settings = make_settings()
    settings.auth_jwks_min_refresh_seconds = 3600
    v = TokenVerifier(settings, http=idp.client())
    await v.verify(idp.token("k1"))
    idp.add_rsa("rogue", publish=False)
    for _ in range(5):
        with pytest.raises(InvalidTokenError, match="signing key"):
            await v.verify(idp.token("rogue"))
    assert idp.requests.count("https://idp.example.com/jwks") == 1


async def test_rfc8414_discovery_fallback(idp: FakeIdp) -> None:
    idp.discovery = "rfc8414"
    assert (await verifier_for(idp).verify(idp.token())).subject == "agent-1"


async def test_metadata_naming_another_issuer_is_refused(idp: FakeIdp) -> None:
    idp.metadata_issuer = "https://evil.example.com"
    with pytest.raises(InvalidTokenError, match="signing key"):
        await verifier_for(idp).verify(idp.token())


async def test_explicit_jwks_uri_skips_discovery(idp: FakeIdp) -> None:
    v = verifier_for(idp, jwks_uri="https://idp.example.com/jwks")
    await v.verify(idp.token())
    assert idp.requests == ["https://idp.example.com/jwks"]


def test_discovery_urls_for_path_issuer() -> None:
    assert discovery_urls("https://idp.example.com/realms/acme/") == [
        "https://idp.example.com/realms/acme/.well-known/openid-configuration",
        "https://idp.example.com/.well-known/oauth-authorization-server/realms/acme",
    ]


# -- configuration ------------------------------------------------------------ #


@pytest.mark.parametrize("alg", ["HS256", "none"])
def test_symmetric_and_none_algorithms_cannot_be_configured(alg: str) -> None:
    with pytest.raises(ValidationError):
        TrustedIssuer(issuer=ISSUER, algorithms=[alg])


def test_auth_cannot_be_disabled_by_accident() -> None:
    with pytest.raises(ValidationError, match="ALLOW_INSECURE_DEV"):
        Settings(auth_enabled=False)
    assert Settings(auth_enabled=False, allow_insecure_dev=True).auth_enabled is False


def test_issuers_parse_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "GBMS_AUTH_ISSUERS",
        f'[{{"issuer": "{ISSUER}", "claims": {{"roles": ["groups"]}}}}]',
    )
    settings = Settings()
    assert settings.auth_issuers[0].claims.roles == ["groups"]
    assert "GBMS_AUTH_ISSUERS is empty" not in " ".join(settings.startup_problems())


def test_startup_problems_flag_empty_issuers() -> None:
    assert any("AUTH_ISSUERS" in p for p in Settings().startup_problems())


# -- HTTP enforcement --------------------------------------------------------- #


def http_app(idp: FakeIdp, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    settings = make_settings()
    monkeypatch.setattr(
        "gaussia_brains_mcp_server.auth.http.get_settings", lambda: settings
    )
    app = FastAPI()
    app.state.token_verifier = TokenVerifier(settings, http=idp.client())
    app.include_router(metadata_router(settings))

    @app.get("/v1/search")
    async def _search(
        principal: Annotated[Principal, Depends(require_scopes(scopes.READ))],
    ):
        return {"sub": principal.subject}

    return TestClient(app)


def test_missing_token_gets_discovery_challenge(idp: FakeIdp, monkeypatch) -> None:
    response = http_app(idp, monkeypatch).get("/v1/search")
    assert response.status_code == 401
    challenge = response.headers["www-authenticate"]
    assert challenge.startswith("Bearer ")
    assert (
        'resource_metadata="https://brains.example.com/.well-known/oauth-protected-resource"'
        in challenge
    )
    assert "error=" not in challenge  # RFC 6750 §3.1: no error without credentials


def test_invalid_token_gets_invalid_token_error(idp: FakeIdp, monkeypatch) -> None:
    response = http_app(idp, monkeypatch).get(
        "/v1/search", headers={"Authorization": "Bearer nope"}
    )
    assert response.status_code == 401
    assert 'error="invalid_token"' in response.headers["www-authenticate"]


def test_insufficient_scope_is_403_with_scope_hint(idp: FakeIdp, monkeypatch) -> None:
    token = idp.token(scope=scopes.CATALOG)
    response = http_app(idp, monkeypatch).get(
        "/v1/search", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 403
    challenge = response.headers["www-authenticate"]
    assert 'error="insufficient_scope"' in challenge
    assert f'scope="{scopes.READ}"' in challenge


def test_sufficient_scope_passes(idp: FakeIdp, monkeypatch) -> None:
    token = idp.token(realm_access={"roles": ["brain-reader"]})
    response = http_app(idp, monkeypatch).get(
        "/v1/search", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200
    assert response.json() == {"sub": "agent-1"}


def test_protected_resource_metadata_document(idp: FakeIdp, monkeypatch) -> None:
    response = http_app(idp, monkeypatch).get("/.well-known/oauth-protected-resource")
    assert response.status_code == 200
    body = response.json()
    assert body["resource"] == RESOURCE
    assert body["authorization_servers"] == [ISSUER]
    assert set(body["scopes_supported"]) == set(scopes.ALL)


def test_resource_metadata_url_inserts_well_known_before_path() -> None:
    assert (
        resource_metadata_url("https://h.example.com/brains/")
        == "https://h.example.com/.well-known/oauth-protected-resource/brains"
    )
    assert (
        metadata_route_path("https://h.example.com/brains")
        == "/.well-known/oauth-protected-resource/brains"
    )


# -- MCP adapter -------------------------------------------------------------- #


async def test_mcp_adapter_round_trips_principal(idp: FakeIdp) -> None:
    adapter = McpTokenVerifier(verifier_for(idp))
    access = await adapter.verify_token(idp.token(scope=scopes.READ, azp="runtime"))
    assert access is not None
    assert access.client_id == "runtime"
    assert access.resource == RESOURCE
    assert access.scopes == [scopes.READ]
    assert principal_from_access_token(access).subject == "agent-1"
    assert await adapter.verify_token("garbage") is None
