"""Namespace-index reader with server-verified signer enrichment.

Brains are listed from the registry namespace index (``<ns>/index:v1``), the
same mechanism agentspaces use. Each entry is an OCI referrer of the index
anchor; this module reads ``kind=brain`` entries, resolves the advertised
digest, verifies the publisher signature and returns both pinned and follow
MCP URLs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger

from gaussia_brains_mcp_server.client import registry_client
from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.signatures import get_signature_verifier

INDEX_ARTIFACT_TYPE = "application/vnd.alquimia.registry.index.v1"
ENTRY_ARTIFACT_TYPE = "application/vnd.alquimia.registry.index.entry.v1"
INDEX_TAG = "v1"

_ANNOTATION_PREFIX = "ai.alquimia.registry.index"


def _annotation(key: str) -> str:
    return f"{_ANNOTATION_PREFIX}.{key}"


@dataclass(frozen=True)
class CatalogEntry:
    """One brain discovered in a namespace index."""

    reference: str
    tag: str
    digest: str
    description: str
    published_at: str | None
    claimed_author: str | None
    signature: dict[str, Any]
    modules: list[str]
    mcp_pinned: str
    mcp_follow: str


class Catalog:
    """Read namespace indexes and enrich their entries."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client_factory: Any = registry_client,
    ) -> None:
        self.settings: Settings | None = settings or None  # resolved lazily
        self._client_factory = client_factory
        self._sig: Any | None = None

    def _settings(self) -> Settings:
        if self.settings is None:
            from gaussia_brains_mcp_server.config import get_settings

            self.settings = get_settings()
        return self.settings  # type: ignore[return-value]

    def _verifier(self) -> Any:
        if self._sig is None:
            self._sig = get_signature_verifier(self._settings())
        return self._sig

    def _client(self) -> Any:
        return self._client_factory(pinned=True)

    @staticmethod
    def _split_namespace(namespace: str) -> tuple[str, str]:
        """``registry.example.com/acme`` -> (host, namespace-path)."""
        host, slash, path = namespace.partition("/")
        return host, path

    @staticmethod
    def _entry_reference(namespace: str, repo: str) -> str | None:
        """Return the full reference if *repo* belongs to *namespace*, else None."""
        host, ns_path = Catalog._split_namespace(namespace)
        if not ns_path:
            return f"{host}/{repo}"
        if repo == ns_path or repo.startswith(f"{ns_path}/"):
            return f"{host}/{repo}"
        return None

    async def _signature_summary(
        self, reference: str, digest: str
    ) -> dict[str, Any]:
        report = self._verifier().inspect(f"{reference}@{digest}")
        return report.summary()

    async def list_entries(
        self,
        namespace: str,
        *,
        base_url: str,
        limit: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[CatalogEntry], str | None]:
        """Read one namespace index and return its brain entries."""
        client = self._client()
        index_reference = f"{namespace}/index"
        document, index_digest = await client.fetch_manifest(index_reference, INDEX_TAG)
        referrers = await client.referrers(
            index_reference, index_digest, artifact_type=ENTRY_ARTIFACT_TYPE
        )

        # Pagination is cursor-based on referrer digest; for now we page by digest.
        entries: list[CatalogEntry] = []
        seen = 0
        started = cursor is None
        next_cursor: str | None = None

        for descriptor in referrers:
            digest = descriptor.digest
            if not started:
                if digest == cursor:
                    started = True
                continue

            annotations = descriptor.annotations or {}
            if annotations.get(_annotation("kind")) != "brain":
                continue

            repo = annotations.get(_annotation("repo"), "")
            tag = annotations.get(_annotation("tag"), "latest")
            entry_digest = annotations.get(_annotation("digest"), "")
            description = annotations.get(_annotation("description"), "")
            published_at = annotations.get(_annotation("published-at"))
            claimed_author = annotations.get(_annotation("author"))

            reference = self._entry_reference(namespace, repo)
            if reference is None:
                logger.debug(
                    "Ignoring index entry {} for namespace {}: repo {} is outside the namespace",
                    digest,
                    namespace,
                    repo,
                )
                continue

            if not entry_digest:
                logger.debug("Ignoring index entry {} with no digest", digest)
                continue

            try:
                manifest = await client.resolve(reference, entry_digest)
                modules = [mt.value for mt in manifest.modules]
            except Exception as exc:
                logger.debug(
                    "Could not resolve advertised digest {} for {}: {}",
                    entry_digest,
                    reference,
                    exc,
                )
                continue

            try:
                signature = await self._signature_summary(reference, entry_digest)
            except Exception as exc:
                logger.debug("Signature check failed for {}: {}", reference, exc)
                signature = {"checked": False, "trusted": False, "signers": [], "detail": str(exc)}

            mcp_base = base_url.rstrip("/")
            mcp_pinned = f"{mcp_base}/{reference}@{entry_digest}"
            mcp_follow = f"{mcp_base}/{reference}:{tag}"

            entries.append(
                CatalogEntry(
                    reference=reference,
                    tag=tag,
                    digest=entry_digest,
                    description=description,
                    published_at=published_at,
                    claimed_author=claimed_author,
                    signature=signature,
                    modules=modules,
                    mcp_pinned=mcp_pinned,
                    mcp_follow=mcp_follow,
                )
            )
            seen += 1
            if seen >= limit:
                next_cursor = digest
                break

        return entries, next_cursor

    async def manifest_detail(
        self, ref: str, *, tag: str | None = None
    ) -> dict[str, Any]:
        """Resolve a brain reference and return its manifest-level detail."""
        from gaussia_brains_mcp_server.cache import split_reference

        reference, parsed_tag = split_reference(ref)
        tag = tag or parsed_tag
        client = self._client()
        manifest = await client.resolve(reference, tag)
        digest = client.last_digest or "unknown"
        try:
            signature = await self._signature_summary(reference, digest)
        except Exception as exc:
            signature = {"checked": False, "trusted": False, "signers": [], "detail": str(exc)}

        return {
            "reference": reference,
            "tag": tag,
            "digest": digest,
            "modules": [mt.value for mt in manifest.modules],
            "annotations": manifest.annotations,
            "signature": signature,
        }


def catalog(settings: Settings | None = None) -> Catalog:
    """Factory for a :class:`Catalog`."""
    return Catalog(settings)
