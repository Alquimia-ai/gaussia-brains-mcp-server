from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from gaussia_brains_mcp_server.config import get_settings

RESOURCE = "https://brains.example.com"
ISSUER = "https://idp.example.com/realms/acme"


@pytest.fixture(autouse=True)
def _isolated_settings(monkeypatch: pytest.MonkeyPatch):
    """No ambient GBMS_* variables or .env leak into a test."""
    import os

    for name in list(os.environ):
        if name.startswith("GBMS_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(os.path.dirname(__file__))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@dataclass
class FakeIdp:
    """An issuer serving discovery metadata and a JWKS over an httpx MockTransport."""

    issuer: str = ISSUER
    keys: dict[str, Any] = field(default_factory=dict)
    published: list[str] = field(default_factory=list)
    requests: list[str] = field(default_factory=list)
    discovery: str = "oidc"  # "oidc" | "rfc8414"
    metadata_issuer: str | None = None

    def add_rsa(
        self, kid: str, *, publish: bool = True, alg: str | None = "RS256"
    ) -> None:
        self.keys[kid] = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048),
            alg,
        )
        if publish:
            self.published.append(kid)

    def add_ec(self, kid: str) -> None:
        self.keys[kid] = (ec.generate_private_key(ec.SECP256R1()), "ES256")
        self.published.append(kid)

    def jwks(self) -> dict[str, Any]:
        out = []
        for kid in self.published:
            private, alg = self.keys[kid]
            algo = (
                jwt.algorithms.RSAAlgorithm
                if isinstance(private, rsa.RSAPrivateKey)
                else jwt.algorithms.ECAlgorithm
            )
            jwk = json.loads(algo.to_jwk(private.public_key()))
            jwk["kid"] = kid
            jwk["use"] = "sig"
            if alg:
                jwk["alg"] = alg
            out.append(jwk)
        return {"keys": out}

    def token(
        self,
        kid: str = "k1",
        *,
        alg: str | None = None,
        headers: dict[str, Any] | None = None,
        **claims: Any,
    ) -> str:
        private, key_alg = self.keys[kid]
        now = int(time.time())
        payload = {
            "iss": self.issuer,
            "sub": "agent-1",
            "aud": RESOURCE,
            "iat": now,
            "exp": now + 300,
        }
        payload.update(claims)
        payload = {k: v for k, v in payload.items() if v is not None}
        return jwt.encode(
            payload,
            private,
            algorithm=alg or key_alg or "RS256",
            headers={"kid": kid, **(headers or {})},
        )

    def transport(self) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            self.requests.append(url)
            meta = {
                "issuer": self.metadata_issuer or self.issuer,
                "jwks_uri": "https://idp.example.com/jwks",
            }
            if (
                url == f"{self.issuer}/.well-known/openid-configuration"
                and self.discovery == "oidc"
            ):
                return httpx.Response(200, json=meta)
            if (
                url
                == "https://idp.example.com/.well-known/oauth-authorization-server/realms/acme"
                and self.discovery == "rfc8414"
            ):
                return httpx.Response(200, json=meta)
            if url == "https://idp.example.com/jwks":
                return httpx.Response(200, json=self.jwks())
            return httpx.Response(404)

        return httpx.MockTransport(handler)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport())


@pytest.fixture
def idp() -> FakeIdp:
    fake = FakeIdp()
    fake.add_rsa("k1")
    return fake
