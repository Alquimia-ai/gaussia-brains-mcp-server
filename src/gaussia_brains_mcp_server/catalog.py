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

from gaussia_brains_mcp_server.client import (
    ENTRY_ARTIFACT_TYPE,
    INDEX_TAG,
    registry_client,
)
from gaussia_brains_mcp_server.config import Settings, SignaturePolicy
from gaussia_brains_mcp_server.repo import expand_namespace
from gaussia_brains_mcp_server.signatures import get_signature_verifier


def _annotation(key: str) -> str:
    return f"ai.alquimia.registry.index.{key}"


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
        settings = self._settings()
        if settings.signature_policy is SignaturePolicy.OFF:
            return {"checked": False, "trusted": False, "signers": [], "detail": None}
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
        settings = self._settings()
        try:
            namespace = expand_namespace(
                namespace, registry_default=settings.registry_default
            )
        except ValueError:
            pass
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

    async def publish(
        self,
        ref: str,
        *,
        tag: str | None = None,
        description: str = "",
        author: str = "",
        published_at: str = "",
    ) -> dict[str, Any]:
        """Resolve *ref* and attach a ``kind=brain`` entry to its namespace index."""
        from gaussia_brains_mcp_server.cache import split_reference
        from gaussia_brains_mcp_server.repo import parse_brain_ref

        settings = self._settings()
        expanded = settings.expand_brain_ref(ref)
        reference, parsed_tag = split_reference(expanded)
        tag = tag or parsed_tag
        brain_ref = parse_brain_ref(
            ref,
            registry_default=settings.registry_default,
            namespace_default=settings.registry_namespace_brains,
        )
        namespace = f"{brain_ref.registry}/{brain_ref.namespace}"

        client = self._client()
        manifest = await client.resolve(reference, tag)
        digest = client.last_digest or "unknown"

        entry_digest = await client.attach_index_entry(
            namespace=namespace,
            repo=reference,
            tag=tag,
            digest=digest,
            description=description,
            author=author,
            published_at=published_at,
        )
        return {
            "reference": reference,
            "tag": tag,
            "digest": digest,
            "namespace": namespace,
            "entry_digest": entry_digest,
            "modules": [mt.value for mt in manifest.modules],
        }

    async def unpublish(self, ref: str, *, tag: str | None = None) -> dict[str, Any]:
        """Detach the index entry advertised for ``ref:tag`` from its namespace index."""
        from gaussia_brains_mcp_server.cache import split_reference
        from gaussia_brains_mcp_server.repo import parse_brain_ref

        settings = self._settings()
        expanded = settings.expand_brain_ref(ref)
        reference, parsed_tag = split_reference(expanded)
        tag = tag or parsed_tag
        brain_ref = parse_brain_ref(
            ref,
            registry_default=settings.registry_default,
            namespace_default=settings.registry_namespace_brains,
        )
        namespace = f"{brain_ref.registry}/{brain_ref.namespace}"

        client = self._client()
        removed = await client.detach_index_entry(namespace, reference, tag)
        return {"reference": reference, "tag": tag, "namespace": namespace, "removed": removed}

    async def list_tags(self, ref: str) -> list[str]:
        """Return every tag published for *ref* (repository-level, unfiltered)."""
        from gaussia_brains_mcp_server.cache import split_reference

        settings = self._settings()
        expanded = settings.expand_brain_ref(ref)
        reference, tag = split_reference(expanded)
        client = self._client()
        return await client.list_tags(reference)

    async def reindex(
        self,
        namespace: str | None = None,
        refs: list[str] | None = None,
    ) -> dict[str, Any]:
        """Rebuild index entries.

        With *refs*, resolve and attach an entry for each one. Otherwise walk
        the existing entries of *namespace* and re-attach them, refreshing their
        metadata.
        """
        settings = self._settings()
        if namespace or not refs:
            namespace = expand_namespace(
                namespace
                or f"{settings.registry_default.rstrip('/')}/{settings.registry_namespace_brains}",
                registry_default=settings.registry_default,
            )
        results: list[dict[str, Any]] = []

        if refs:
            for ref in refs:
                try:
                    info = await self.publish(ref)
                    results.append({"ref": ref, "status": "published", **info})
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Reindex failed for {}: {}", ref, exc)
                    results.append({"ref": ref, "status": "failed", "error": str(exc)})
            return {"namespace": namespace, "results": results}

        # No explicit refs: refresh every entry already present in the index.
        client = self._client()
        target_ns = namespace or settings.effective_catalog_namespaces()[0]
        entries = await client.discover_index_entries(target_ns)
        for entry in entries:
            repo = entry.get("repo", "")
            tag = entry.get("tag", "latest")
            digest = entry.get("subject_digest") or entry.get("digest", "")
            if not repo or not digest:
                continue
            try:
                await client.attach_index_entry(
                    namespace=target_ns,
                    repo=repo,
                    tag=tag,
                    digest=digest,
                    description=entry.get("description", ""),
                    author=entry.get("author", ""),
                    published_at=entry.get("published_at") or entry.get("published-at", ""),
                    replace=True,
                )
                results.append({"repo": repo, "tag": tag, "status": "refreshed"})
            except Exception as exc:  # noqa: BLE001
                logger.warning("Refresh failed for {}:{}: {}", repo, tag, exc)
                results.append({"repo": repo, "tag": tag, "status": "failed", "error": str(exc)})
        return {"namespace": namespace, "refreshed": len(results), "results": results}


def catalog(settings: Settings | None = None) -> Catalog:
    """Factory for a :class:`Catalog`."""
    return Catalog(settings)
