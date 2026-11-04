"""Issuer signing keys: discovery (OIDC / RFC 8414) and a JWKS cache (RFC 7517)."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx
import jwt
from loguru import logger

from gaussia_brains_mcp_server.config import TrustedIssuer


class KeyResolutionError(Exception):
    """No key could be found or fetched to verify a token's signature."""


def discovery_urls(issuer: str) -> list[str]:
    """Metadata URLs to try for *issuer*, in order.

    OpenID Connect Discovery appends to the issuer; RFC 8414 inserts the
    well-known segment between host and path. They differ only for issuers
    with a path (Keycloak realms, multi-tenant IdPs), so both are tried.
    """
    trimmed = issuer.rstrip("/")
    parts = urlsplit(trimmed)
    origin = f"{parts.scheme}://{parts.netloc}"
    urls = [f"{trimmed}/.well-known/openid-configuration"]
    rfc8414 = f"{origin}/.well-known/oauth-authorization-server{parts.path}"
    if rfc8414 not in urls:
        urls.append(rfc8414)
    return urls


@dataclass(frozen=True)
class SigningKey:
    """A verification key and the algorithm its JWK pins, if any (RFC 7517 §4.4)."""

    key: Any
    kid: str | None
    alg: str | None


class IssuerKeys:
    """The signing keys of one trusted issuer, fetched lazily and cached.

    Keys are refreshed after ``ttl`` and whenever a token names a ``kid`` not
    held -- the normal shape of a key rotation -- but no more often than
    ``min_refresh`` seconds, so a flood of tokens with made-up ``kid`` values
    cannot turn this service into a load generator against the IdP.
    """

    def __init__(
        self,
        issuer: TrustedIssuer,
        http: httpx.AsyncClient,
        *,
        ttl: float,
        min_refresh: float,
    ) -> None:
        self.issuer = issuer
        self._http = http
        self._ttl = ttl
        self._min_refresh = min_refresh
        self._keys: dict[str | None, SigningKey] = {}
        self._jwks_uri: str | None = issuer.jwks_uri
        self._fetched_at = 0.0
        self._lock = asyncio.Lock()

    async def get_key(self, kid: str | None) -> SigningKey:
        now = time.monotonic()
        if not self._keys or now - self._fetched_at >= self._ttl:
            await self._refresh(force=False)
        key = self._lookup(kid)
        if key is not None:
            return key
        if time.monotonic() - self._fetched_at >= self._min_refresh:
            await self._refresh(force=True)
            key = self._lookup(kid)
            if key is not None:
                return key
        raise KeyResolutionError(
            f"no signing key {kid!r} published by {self.issuer.issuer}"
        )

    def _lookup(self, kid: str | None) -> SigningKey | None:
        if kid is not None:
            return self._keys.get(kid)
        # A token without `kid` is only unambiguous against a single key.
        if len(self._keys) == 1:
            return next(iter(self._keys.values()))
        return None

    async def _refresh(self, *, force: bool) -> None:
        async with self._lock:
            # Another waiter may have refreshed while this one queued.
            age = time.monotonic() - self._fetched_at
            if self._keys and (
                age < self._min_refresh or (not force and age < self._ttl)
            ):
                return
            try:
                uri = self._jwks_uri or await self._discover_jwks_uri()
                self._jwks_uri = uri
                response = await self._http.get(uri)
                response.raise_for_status()
                keys = self._parse(response.json())
            except Exception as exc:  # noqa: BLE001
                if self._keys:
                    # Keep verifying with the held keys: an IdP blip must not
                    # reject every token signed by a key we already trust.
                    logger.warning(
                        "JWKS refresh for {} failed, keeping {} held keys: {}",
                        self.issuer.issuer,
                        len(self._keys),
                        exc,
                    )
                    # Retry after `min_refresh`, not a whole TTL.
                    self._fetched_at = time.monotonic() - self._ttl + self._min_refresh
                    return
                raise KeyResolutionError(
                    f"could not fetch signing keys of {self.issuer.issuer}: {exc}"
                ) from exc
            self._keys = keys
            self._fetched_at = time.monotonic()
            logger.debug("Loaded {} signing keys for {}", len(keys), self.issuer.issuer)

    async def _discover_jwks_uri(self) -> str:
        last_error: Exception | None = None
        for url in discovery_urls(self.issuer.issuer):
            try:
                response = await self._http.get(url)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                metadata = response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                continue
            # RFC 8414 §3.3 / OIDC Discovery §4.3: the metadata must name the
            # issuer it was fetched for, or it is not that issuer's metadata.
            if metadata.get("issuer") != self.issuer.issuer:
                raise KeyResolutionError(
                    f"metadata at {url} names issuer {metadata.get('issuer')!r}, "
                    f"expected {self.issuer.issuer!r}"
                )
            jwks_uri = metadata.get("jwks_uri")
            if not jwks_uri:
                raise KeyResolutionError(f"metadata at {url} has no jwks_uri")
            return str(jwks_uri)
        raise KeyResolutionError(
            f"no authorization server metadata found for {self.issuer.issuer}"
            + (f": {last_error}" if last_error else "")
        )

    def _parse(self, document: dict) -> dict[str | None, SigningKey]:
        keys: dict[str | None, SigningKey] = {}
        for raw in document.get("keys") or []:
            if raw.get("use", "sig") != "sig":
                continue
            try:
                key = jwt.PyJWK.from_dict(raw)
            except jwt.PyJWKError as exc:
                logger.debug(
                    "Skipping unusable JWK from {}: {}", self.issuer.issuer, exc
                )
                continue
            keys[key.key_id] = SigningKey(
                key=key.key, kid=key.key_id, alg=raw.get("alg")
            )
        if not keys:
            raise KeyResolutionError(
                f"{self.issuer.issuer} publishes no usable signing keys"
            )
        return keys
