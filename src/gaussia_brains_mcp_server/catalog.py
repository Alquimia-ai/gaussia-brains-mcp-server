"""Namespace-index reader with server-verified signer enrichment.

Brains are listed from the registry namespace index (``<ns>/index:v1``), the
same mechanism agentspaces use. Each entry is an OCI referrer of the index
anchor carrying repo, tag, digest, description and modules as annotations, so
one ``oras discover`` on the anchor lists a whole namespace. Brain manifests are
read only for legacy entries published without a ``modules`` annotation, and
that result is cached by digest (a digest's content never changes).
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from boltzmann.distribution.manifest import Descriptor
from loguru import logger

from gaussia_brains_mcp_server.client import registry_client, repo_path
from gaussia_brains_mcp_server.config import Settings, SignaturePolicy
from gaussia_brains_mcp_server.repo import expand_namespace
from gaussia_brains_mcp_server.signatures import get_signature_verifier


def _annotation(key: str) -> str:
    return f"ai.alquimia.registry.index.{key}"


class _IndexCache:
    """Process-wide memo of index listings (short TTL) and modules per digest."""

    def __init__(self, max_digests: int = 4096) -> None:
        self._lock = threading.Lock()
        self._listings: dict[str, tuple[float, list[Descriptor]]] = {}
        self._modules: OrderedDict[str, list[str]] = OrderedDict()
        self._max = max_digests

    def listing(self, namespace: str, ttl: float) -> list[Descriptor] | None:
        if ttl <= 0:
            return None
        with self._lock:
            hit = self._listings.get(namespace)
        if hit is None or time.monotonic() - hit[0] >= ttl:
            return None
        return list(hit[1])

    def store_listing(self, namespace: str, entries: list[Descriptor]) -> None:
        with self._lock:
            self._listings[namespace] = (time.monotonic(), entries)

    def invalidate(self, namespace: str | None = None) -> None:
        with self._lock:
            if namespace is None:
                self._listings.clear()
            else:
                self._listings.pop(namespace, None)

    def modules(self, digest: str) -> list[str] | None:
        with self._lock:
            found = self._modules.get(digest)
            if found is not None:
                self._modules.move_to_end(digest)
            return list(found) if found is not None else None

    def store_modules(self, digest: str, modules: list[str]) -> None:
        with self._lock:
            self._modules[digest] = modules
            self._modules.move_to_end(digest)
            while len(self._modules) > self._max:
                self._modules.popitem(last=False)


_index_cache = _IndexCache()


def _split_modules(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    return [m for m in (part.strip() for part in raw.split(",")) if m]


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
        """Return the full reference if *repo* belongs to *namespace*, else None.

        *repo* may be registry-relative (``brains/x``) or fully qualified.
        """
        host, ns_path = Catalog._split_namespace(namespace)
        repo = repo_path(namespace, repo)
        if not ns_path:
            return f"{host}/{repo}"
        if repo == ns_path or repo.startswith(f"{ns_path}/"):
            return f"{host}/{repo}"
        return None

    async def _signature_summary(self, reference: str, digest: str) -> dict[str, Any]:
        settings = self._settings()
        if settings.signature_policy is SignaturePolicy.OFF:
            return {"checked": False, "trusted": False, "signers": [], "detail": None}
        report = await asyncio.to_thread(
            self._verifier().inspect, f"{reference}@{digest}"
        )
        return report.summary()

    async def _index(self, client: Any, namespace: str) -> list[Descriptor]:
        """The namespace's index entries: one ``oras discover`` on the anchor."""
        ttl = self._settings().catalog_index_ttl_seconds
        cached = _index_cache.listing(namespace, ttl)
        if cached is not None:
            return cached
        entries = await client.index_entries(namespace)
        _index_cache.store_listing(namespace, entries)
        return entries

    async def _modules(
        self, client: Any, reference: str, digest: str, advertised: str | None
    ) -> list[str]:
        """Modules from the entry annotation; legacy entries read the manifest."""
        modules = _split_modules(advertised)
        if modules is not None:
            return modules
        cached = _index_cache.modules(digest)
        if cached is not None:
            return cached
        modules = await client.manifest_modules(reference, digest)
        _index_cache.store_modules(digest, modules)
        return modules

    async def _enrich(
        self,
        client: Any,
        namespace: str,
        descriptor: Descriptor,
        *,
        base_url: str,
        gate: asyncio.Semaphore,
    ) -> CatalogEntry | None:
        """Turn one index entry into a :class:`CatalogEntry`, or None to skip it."""
        annotations = descriptor.annotations or {}
        repo = annotations.get(_annotation("repo"), "")
        tag = annotations.get(_annotation("tag"), "latest")
        entry_digest = annotations.get(_annotation("digest"), "")
        reference = self._entry_reference(namespace, repo)
        if reference is None:
            logger.debug(
                "Ignoring index entry {} for namespace {}: repo {} is outside the namespace",
                descriptor.digest,
                namespace,
                repo,
            )
            return None
        if not entry_digest:
            logger.debug("Ignoring index entry {} with no digest", descriptor.digest)
            return None

        async with gate:
            try:
                modules = await self._modules(
                    client,
                    reference,
                    entry_digest,
                    annotations.get(_annotation("modules")),
                )
            except Exception as exc:
                logger.debug(
                    "Could not resolve advertised digest {} for {}: {}",
                    entry_digest,
                    reference,
                    exc,
                )
                return None
            try:
                signature = await self._signature_summary(reference, entry_digest)
            except Exception as exc:
                logger.debug("Signature check failed for {}: {}", reference, exc)
                signature = {
                    "checked": False,
                    "trusted": False,
                    "signers": [],
                    "detail": str(exc),
                }

        mcp_base = base_url.rstrip("/")
        return CatalogEntry(
            reference=reference,
            tag=tag,
            digest=entry_digest,
            description=annotations.get(_annotation("description"), ""),
            published_at=annotations.get(_annotation("published-at")),
            claimed_author=annotations.get(_annotation("author")),
            signature=signature,
            modules=modules,
            mcp_pinned=f"{mcp_base}/{reference}@{entry_digest}",
            mcp_follow=f"{mcp_base}/{reference}:{tag}",
        )

    async def list_entries(
        self,
        namespace: str,
        *,
        base_url: str,
        limit: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[CatalogEntry], str | None]:
        """Read one namespace index and return its brain entries.

        Everything comes from the index anchor's entries; entries are enriched
        concurrently, in index order. The cursor is the digest of the last entry
        returned.
        """
        settings = self._settings()
        try:
            namespace = expand_namespace(
                namespace, registry_default=settings.registry_default
            )
        except ValueError:
            pass
        client = self._client()
        descriptors = await self._index(client, namespace)

        candidates = [
            d
            for d in descriptors
            if (d.annotations or {}).get(_annotation("kind")) == "brain"
        ]
        if cursor is not None:
            positions = [i for i, d in enumerate(candidates) if str(d.digest) == cursor]
            candidates = candidates[positions[0] + 1 :] if positions else []

        gate = asyncio.Semaphore(settings.catalog_concurrency)
        entries: list[CatalogEntry] = []
        next_cursor: str | None = None
        # Enrich in batches of what is still needed: entries that fail to
        # resolve are skipped, so a batch may come back short.
        while candidates and len(entries) < limit:
            batch, candidates = (
                candidates[: limit - len(entries)],
                candidates[limit - len(entries) :],
            )
            enriched = await asyncio.gather(
                *(
                    self._enrich(client, namespace, d, base_url=base_url, gate=gate)
                    for d in batch
                )
            )
            for descriptor, entry in zip(batch, enriched, strict=True):
                if entry is not None:
                    entries.append(entry)
                    if len(entries) >= limit:
                        next_cursor = str(descriptor.digest)
        return entries, next_cursor

    async def manifest_detail(
        self, ref: str, *, tag: str | None = None, base_url: str | None = None
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
            signature = {
                "checked": False,
                "trusted": False,
                "signers": [],
                "detail": str(exc),
            }

        mcp_base = (base_url or self._settings().mcp_path).rstrip("/")
        mcp_pinned = f"{mcp_base}/{reference}@{digest}"
        mcp_follow = f"{mcp_base}/{reference}:{tag}"

        return {
            "reference": reference,
            "tag": tag,
            "digest": digest,
            "modules": [mt.value for mt in manifest.modules],
            "annotations": manifest.annotations,
            "signature": signature,
            "mcp": {
                "pinned": mcp_pinned,
                "follow": mcp_follow,
            },
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
            modules=[mt.value for mt in manifest.modules],
        )
        _index_cache.invalidate(namespace)
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
        _index_cache.invalidate(namespace)
        return {
            "reference": reference,
            "tag": tag,
            "namespace": namespace,
            "removed": removed,
        }

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
        try:
            entries = await client.discover_index_entries(target_ns)
            for entry in entries:
                repo = entry.get("repo", "")
                tag = entry.get("tag", "latest")
                digest = entry.get("subject_digest") or entry.get("digest", "")
                if not repo or not digest:
                    continue
                try:
                    reference = self._entry_reference(target_ns, repo) or repo
                    modules = await self._modules(
                        client, reference, digest, entry.get("modules") or None
                    )
                    await client.attach_index_entry(
                        namespace=target_ns,
                        repo=repo,
                        tag=tag,
                        digest=digest,
                        description=entry.get("description", ""),
                        author=entry.get("author", ""),
                        published_at=entry.get("published_at")
                        or entry.get("published-at", ""),
                        modules=modules,
                        replace=True,
                    )
                    results.append({"repo": repo, "tag": tag, "status": "refreshed"})
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Refresh failed for {}:{}: {}", repo, tag, exc)
                    results.append(
                        {
                            "repo": repo,
                            "tag": tag,
                            "status": "failed",
                            "error": str(exc),
                        }
                    )
        finally:
            _index_cache.invalidate(target_ns)
        return {"namespace": namespace, "refreshed": len(results), "results": results}


def catalog(settings: Settings | None = None) -> Catalog:
    """Factory for a :class:`Catalog`."""
    return Catalog(settings)
