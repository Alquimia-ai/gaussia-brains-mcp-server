"""Pydantic settings for the Gaussia Brains MCP Server.

Every variable carries the ``GBMS_`` prefix. The service deliberately reads no
``ALQUIMIA_*`` / ``BRAIN_*`` runtime variables and no SPIFFE settings: it has
its own configuration surface and its own lifecycle.

Nested values (the trusted issuer list) are given as JSON, e.g.::

    GBMS_AUTH_ISSUERS='[{"issuer": "https://idp.example.com/realms/acme",
                         "claims": {"roles": ["realm_access.roles"]},
                         "role_scopes": {"brain-reader": ["brains:search", "brains:read"]}}]'
"""

from __future__ import annotations

import re
from enum import StrEnum
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: JOSE algorithms never accepted, whatever an issuer entry says. ``none`` is
#: unsigned; HS* would need a secret shared with every issuer, which defeats
#: verifying tokens from an issuer this service does not control.
FORBIDDEN_ALGORITHMS = frozenset({"none", "HS256", "HS384", "HS512"})
DEFAULT_ALGORITHMS = ("RS256", "PS256", "ES256", "EdDSA")


class UnsignedPolicy(StrEnum):
    """Verification policy for unsigned brain snapshots."""

    WARN = "warn"
    REFUSE = "refuse"
    PERMIT = "permit"


class SignaturePolicy(StrEnum):
    """Cosign publisher-signature policy applied when resolving a manifest."""

    OFF = "off"
    WARN = "warn"
    REQUIRED = "required"


class ClaimMapping(BaseModel):
    """Where an issuer puts the claims this service authorizes on.

    Each entry is a dotted path into the token payload (``realm_access.roles``,
    ``resource_access.my-client.roles``). Lists are tried in order and merged,
    so one mapping can serve issuers that use ``scope`` and ones that use ``scp``.
    """

    subject: str = "sub"
    scopes: list[str] = Field(default_factory=lambda: ["scope", "scp"])
    roles: list[str] = Field(default_factory=lambda: ["roles"])
    client_id: list[str] = Field(default_factory=lambda: ["client_id", "azp"])


class TrustedSigner(BaseModel):
    """A keyless (Sigstore/Fulcio) signer whose brains are trusted.

    A brain's signing certificate carries the OIDC issuer that authenticated
    the publisher and the identity it authenticated (an email, or a CI
    workflow URI). Both must match one entry. ``subject`` is an exact match,
    ``subject_regex`` a full-match regular expression; give exactly one.
    """

    issuer: str
    subject: str | None = None
    subject_regex: str | None = None

    @model_validator(mode="after")
    def _exactly_one_subject_form(self) -> TrustedSigner:
        if (self.subject is None) == (self.subject_regex is None):
            raise ValueError("give exactly one of subject or subject_regex")
        if self.subject_regex is not None:
            re.compile(self.subject_regex)
        return self

    def matches(self, issuer: str | None, subject: str | None) -> bool:
        if issuer != self.issuer or not subject:
            return False
        if self.subject is not None:
            return subject == self.subject
        return re.fullmatch(self.subject_regex or "", subject) is not None


class TrustedIssuer(BaseModel):
    """One OAuth 2.0 / OIDC issuer whose JWT access tokens are accepted."""

    issuer: str = Field(
        ..., description="Exact `iss` value (RFC 8414 / OIDC Discovery)."
    )
    #: Accepted `aud` values. Empty means ``[Settings.auth_resource]`` (RFC 8707).
    audiences: list[str] = Field(default_factory=list)
    #: Explicit JWKS URL. Unset means discovered from the issuer's metadata.
    jwks_uri: str | None = None
    algorithms: list[str] = Field(default_factory=lambda: list(DEFAULT_ALGORITHMS))
    #: Required JOSE ``typ`` header, e.g. ``at+jwt`` (RFC 9068). Unset skips the check.
    required_typ: str | None = None
    claims: ClaimMapping = Field(default_factory=ClaimMapping)
    #: Scopes granted by role, for issuers that express permissions as roles
    #: (Keycloak realm roles, Kubernetes groups) instead of OAuth scopes.
    role_scopes: dict[str, list[str]] = Field(default_factory=dict)

    @field_validator("issuer")
    @classmethod
    def _issuer_is_https_url(cls, value: str) -> str:
        if not value.startswith(("https://", "http://")):
            raise ValueError(f"issuer must be an http(s) URL, got {value!r}")
        return value

    @field_validator("algorithms")
    @classmethod
    def _no_forbidden_algorithms(cls, value: list[str]) -> list[str]:
        bad = sorted(set(value) & FORBIDDEN_ALGORITHMS)
        if bad:
            raise ValueError(
                f"algorithms {bad} are not accepted; use asymmetric algorithms"
            )
        if not value:
            raise ValueError("algorithms must not be empty")
        return value


class Settings(BaseSettings):
    """Server configuration (environment first, then ``.env``)."""

    model_config = SettingsConfigDict(
        env_prefix="GBMS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Server ───────────────────────────────────────────────────────────
    app_name: str = "gaussia-brains-mcp-server"
    log_level: str = "INFO"
    #: One listener serves REST (``/v1``), the RFC 9728 metadata and MCP
    #: (Streamable HTTP under ``mcp_path``), so both share one OAuth resource.
    host: str = "0.0.0.0"
    port: int = 8000
    mcp_path: str = "/mcp"
    mcp_transport: Literal["stdio", "streamable-http"] = "streamable-http"
    #: stdio only: the brain the process serves, and the token identifying the caller.
    stdio_brain: str | None = None
    stdio_bearer_token: SecretStr | None = None

    # ── Registry defaults for short brain refs ───────────────────────────
    #: Default registry host used when a brain reference does not include one
    #: (e.g. ``brains/my-brain:latest`` expands to ``registry.alquimia.ai/brains/my-brain:latest``).
    registry_default: str = "registry.alquimia.ai"
    #: Default publishing namespace for short brain references.
    registry_namespace_brains: str = "brains"

    # ── Discovery (namespace indexes) ────────────────────────────────────
    #: ``registry/namespace`` pairs whose index (``<ns>/index:v1``) is listed.
    catalog_namespaces: list[str] = Field(default_factory=list)
    #: ``registry/namespace`` prefixes brains may be served from. Empty means
    #: ``catalog_namespaces``. Both empty refuses every brain: a headless
    #: server must not pull from any registry a caller happens to name.
    allowed_sources: list[str] = Field(default_factory=list)

    def effective_catalog_namespaces(self) -> list[str]:
        """Return configured catalog namespaces, defaulting to the brain namespace."""
        if self.catalog_namespaces:
            return [s.rstrip("/") for s in self.catalog_namespaces]
        return [f"{self.registry_default.rstrip('/')}/{self.registry_namespace_brains}"]

    def effective_sources(self) -> list[str]:
        sources = self.allowed_sources or self.catalog_namespaces
        if sources:
            return [s.rstrip("/") for s in sources]
        return [f"{self.registry_default.rstrip('/')}/{self.registry_namespace_brains}"]

    def source_allowed(self, reference: str) -> bool:
        """Whether *reference* (``registry/ns/repo``, no tag) is in an allowed source."""
        from gaussia_brains_mcp_server.repo import parse_brain_ref

        try:
            expanded = parse_brain_ref(
                reference,
                registry_default=self.registry_default,
                namespace_default=self.registry_namespace_brains,
            )
            candidates = {reference, expanded.reference}
        except ValueError:
            candidates = {reference}
        return any(
            candidate == source or candidate.startswith(source + "/")
            for source in self.effective_sources()
            for candidate in candidates
        )

    def expand_brain_ref(self, ref: str) -> str:
        """Expand a short reference using the configured registry defaults."""
        from gaussia_brains_mcp_server.repo import parse_brain_ref

        return parse_brain_ref(
            ref,
            registry_default=self.registry_default,
            namespace_default=self.registry_namespace_brains,
        ).target

    # ── Brain cache ──────────────────────────────────────────────────────
    cache_ttl_seconds: float = Field(default=300.0, ge=0)
    cache_max_brains: int = Field(default=4, ge=1)
    cache_failure_backoff_seconds: float = Field(default=30.0, ge=0)
    cache_failure_backoff_max_seconds: float = Field(default=300.0, ge=0)

    # ── Snapshot authorship verification (pyboltzmann) ───────────────────
    verify_unsigned: UnsignedPolicy = UnsignedPolicy.REFUSE
    required_signatures: int = Field(default=1, ge=1)
    allow_propose_head: bool = False
    #: Actor id the read-only brain handle is opened as. Must be an address or
    #: a namespaced name (pyboltzmann refuses anything else).
    reader_actor_id: str = "gaussia/brains-reader"

    # ── OCI registry (via the `oras` CLI) ────────────────────────────────
    oras_bin: str = "oras"
    oras_timeout_seconds: int = Field(default=120, ge=1)
    oras_insecure: bool = False
    oras_plain_http: bool = False
    #: Docker-style credentials file (``config.json``) passed to oras.
    registry_config: str | None = None
    registry_username: str | None = None
    registry_password: SecretStr | None = None

    # ── Publisher signatures (cosign, feature-flagged) ───────────────────
    #: ``off``: never checked. ``warn``: served, untrusted ones logged.
    #: ``required``: only brains signed by a trusted signer are served.
    signature_policy: SignaturePolicy = SignaturePolicy.OFF
    #: Keyless signers (OIDC issuer + identity) whose brains are trusted.
    trusted_signers: list[TrustedSigner] = Field(default_factory=list)
    #: Optional key-based alternative (path or KMS URI) for non-Sigstore estates.
    cosign_key: str | None = None
    cosign_bin: str = "cosign"

    # ── Authentication: OAuth 2.0 resource server ────────────────────────
    auth_enabled: bool = True
    #: Must be set alongside ``auth_enabled=false``; refuses to start otherwise.
    allow_insecure_dev: bool = False
    #: This service's resource identifier (RFC 8707 / RFC 9728), e.g.
    #: ``https://brains.example.com``. Default audience for every issuer.
    auth_resource: str = "http://localhost:8000"
    auth_issuers: list[TrustedIssuer] = Field(default_factory=list)
    auth_leeway_seconds: int = Field(default=30, ge=0, le=300)
    auth_jwks_cache_ttl_seconds: int = Field(default=600, ge=1)
    #: Minimum interval between JWKS refetches triggered by an unknown ``kid``.
    auth_jwks_min_refresh_seconds: int = Field(default=30, ge=0)
    auth_http_timeout_seconds: float = Field(default=5.0, gt=0)
    #: Optional link advertised as ``resource_documentation`` (RFC 9728).
    auth_resource_documentation: str | None = None

    @model_validator(mode="after")
    def _insecure_dev_is_explicit(self) -> Settings:
        if not self.auth_enabled and not self.allow_insecure_dev:
            raise ValueError(
                "GBMS_AUTH_ENABLED=false requires GBMS_ALLOW_INSECURE_DEV=true; "
                "authentication is never disabled by accident"
            )
        return self

    @model_validator(mode="after")
    def _signature_policy_has_a_verifier(self) -> Settings:
        if self.signature_policy is SignaturePolicy.OFF:
            return self
        if not self.trusted_signers and not self.cosign_key:
            raise ValueError(
                f"GBMS_SIGNATURE_POLICY={self.signature_policy.value} needs "
                "GBMS_TRUSTED_SIGNERS (keyless) or GBMS_COSIGN_KEY; with neither, "
                "no brain could ever be trusted"
            )
        return self

    def startup_problems(self) -> list[str]:
        """Configuration that is valid to construct but not to serve with."""
        problems: list[str] = []
        if not self.registry_default:
            problems.append("GBMS_REGISTRY_DEFAULT is empty: short brain refs cannot be expanded")
        if not self.registry_namespace_brains:
            problems.append("GBMS_REGISTRY_NAMESPACE_BRAINS is empty: the default brain namespace is unknown")
        if self.auth_enabled and not self.auth_issuers:
            problems.append(
                "GBMS_AUTH_ISSUERS is empty: no token could ever be accepted"
            )
        if self.auth_enabled and not self.auth_resource.startswith("https://"):
            problems.append(
                f"GBMS_AUTH_RESOURCE={self.auth_resource!r} is not https; acceptable "
                "only for local development"
            )
        return problems


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
