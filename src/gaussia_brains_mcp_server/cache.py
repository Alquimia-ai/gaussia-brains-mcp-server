"""Bounded in-memory cache of opened Boltzmann brains.

Re-ported from ``alquimia.boltzmann.cache`` keeping the fixes that module
documents, adapted to a service where every brain lives in memory:

* **Whole brains, keyed by reference.** Brains are headless: there is no
  registration narrowing what is pulled, so every module is installed and the
  key is ``(reference, tag-or-digest)``. Nothing a caller asks for changes it.
* **Single-flight.** A placeholder entry holding its lock is inserted before
  the first pull, and eviction never removes the entry being opened.
* **Revalidation, not re-pulls.** After the TTL only the manifest is resolved;
  blobs are pulled again only when its config digest moved, or when new
  signatures appeared on a governed brain not yet authorized.
* **Staleness beats unavailability.** A registry outage keeps serving the held
  brain, unless the reader's verification policy refuses what is held.
* **Judged under the policy it is served under.** A held brain is re-judged
  when a caller presents a different verification policy.
* **Backoff.** Failed first opens back off exponentially so an outage does not
  spawn a subprocess per request.
* **Pinned digests never revalidate.** ``repo@sha256:...`` is immutable; only a
  ``repo:tag`` connection follows new publishes.

With nothing on disk, a fresh pull cannot detect a rollback (a tag moved to an
ancestor) the way a layout store does; see docs/proposal.md §6.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from boltzmann import Brain, MemoryType
from boltzmann.authenticity import (
    AuthorshipState,
    SnapshotStance,
    UnsignedPolicy,
    VerificationPolicy,
)
from boltzmann.distribution.media_types import (
    ANNOTATION_EMBEDDING_MODEL,
    SIGNATURE_MEDIA_TYPE,
)
from boltzmann.exceptions import AuthenticityError
from loguru import logger

from gaussia_brains_mcp_server.client import (
    BrainManifestRejected,
    is_digest,
    registry_client,
)
from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.store import build_brain, get_index_provider

Key = tuple[str, str]


class BrainUnavailableError(Exception):
    """A brain could not be opened and nothing is held to serve instead."""


class SourceNotAllowedError(Exception):
    """The reference is outside ``GBMS_ALLOWED_SOURCES``."""


@dataclass
class _Entry:
    brain: Brain | None = None
    #: The manifest *config* digest last installed (what revalidation compares).
    manifest_digest: str | None = None
    checked_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    failures: int = 0
    retry_after: float = 0.0
    #: The policy the held brain was accepted under.
    verification: VerificationPolicy | None = None
    #: Signature referrers last seen on the served manifest.
    signatures: frozenset[str] = frozenset()


def split_reference(oci_ref: str, default_tag: str = "latest") -> tuple[str, str]:
    """Split a reference into ``(registry/repo, tag-or-digest)``.

    ``repo@sha256:...`` pins a digest (a tag before it is ignored: the digest
    is what is served). Otherwise a colon only separates a tag when it comes
    after the last ``/``; one before it is a registry port
    (``localhost:5000/brains/x``).
    """
    if "@" in oci_ref:
        name, digest = oci_ref.split("@", 1)
        if not is_digest(digest):
            raise ValueError(f"unsupported digest in {oci_ref!r}")
        reference, _ = split_reference(name, default_tag="")
        return reference, digest
    last_slash = oci_ref.rfind("/")
    colon = oci_ref.rfind(":")
    if colon > last_slash:
        return oci_ref[:colon], oci_ref[colon + 1 :]
    return oci_ref, default_tag


def default_verification(settings: Settings) -> VerificationPolicy:
    return VerificationPolicy(
        unsigned=UnsignedPolicy(settings.verify_unsigned.value),
        required_signatures=settings.required_signatures,
        allow_propose_head=settings.allow_propose_head,
    )


def refuses_held(brain: Brain, verification: VerificationPolicy | None) -> bool:
    """Whether *verification* refuses the head *brain* holds.

    Falling back to what is held is right when the registry is the problem and
    wrong when the reader is: a head this policy would not install is not
    served because a later pull failed.
    """
    try:
        state = brain.authenticate(policy=verification).state
    except AuthenticityError:
        return True
    if state is AuthorshipState.UNAUTHORIZED:
        return True
    return (
        state is AuthorshipState.UNSIGNED
        and verification is not None
        and verification.unsigned is UnsignedPolicy.REFUSE
    )


class BrainCache:
    """Process-wide cache of opened brains."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client_factory: Callable[..., Any] = registry_client,
        brain_factory: Callable[[], Brain] = build_brain,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings or get_settings()
        self._client_factory = client_factory
        self._brain_factory = brain_factory
        self._clock = clock
        self._entries: dict[Key, _Entry] = {}
        # Never held across an await: it guards the dict, not the pulls.
        self._entries_lock = asyncio.Lock()

    # -- public API ---------------------------------------------------------- #

    async def get_or_open(
        self,
        oci_ref: str,
        *,
        tag: str | None = None,
        verification: VerificationPolicy | None = None,
        strict: bool = False,
    ) -> Brain:
        """Return an opened brain, pulling or revalidating only when needed.

        Args:
            oci_ref: ``repo``, ``repo:tag`` or ``repo@sha256:...``.
            tag: Overrides the tag in *oci_ref*.
            verification: Authorship policy; defaults to the configured one.
            strict: Raise an authenticity refusal instead of serving a held head.
        """
        reference, parsed_tag = split_reference(oci_ref)
        tag = tag or parsed_tag
        if not self.settings.source_allowed(reference):
            raise SourceNotAllowedError(
                f"{reference} is not in an allowed source "
                f"({', '.join(self.settings.effective_sources()) or 'none configured'})"
            )
        verification = verification or default_verification(self.settings)
        key: Key = (reference, tag)

        async with self._entries_lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _Entry()
                self._entries[key] = entry
                self._evict_if_needed(protect=key)

        async with entry.lock:
            now = self._clock()
            pull_modules: list[MemoryType] | None = None  # headless: every module

            if entry.brain is None:
                if now < entry.retry_after:
                    raise BrainUnavailableError(
                        f"brain {reference}:{tag} could not be opened ({entry.failures} "
                        f"consecutive failures); retrying in {entry.retry_after - now:.0f}s"
                    )
                try:
                    brain, digest = await self._open(
                        reference, tag, pull_modules, verification
                    )
                except Exception:
                    self._record_failure(entry)
                    raise
                self._install(entry, brain, digest, verification)
                entry.signatures = frozenset()
                return brain

            if strict or entry.verification != verification:
                return await self._refresh(
                    entry, reference, tag, pull_modules, verification, strict=strict
                )

            ttl = self.settings.cache_ttl_seconds
            if is_digest(tag):
                return entry.brain
            if ttl > 0 and now - entry.checked_at >= ttl:
                return await self._revalidate(
                    entry, reference, tag, pull_modules, verification
                )
            return entry.brain

    def invalidate(self, oci_ref: str, tag: str | None = None) -> int:
        """Drop entries for *oci_ref* (every tag unless one is given)."""
        reference, parsed_tag = split_reference(oci_ref, default_tag="")
        tag = tag or parsed_tag or None
        keys = [
            k
            for k in self._entries
            if k[0] == reference and (tag is None or k[1] == tag)
        ]
        for k in keys:
            self._entries.pop(k, None)
        return len(keys)

    def clear(self) -> None:
        self._entries.clear()

    def stats(self) -> dict[str, Any]:
        now = self._clock()
        return {
            "size": len(self._entries),
            "max_size": self.settings.cache_max_brains,
            "ttl_seconds": self.settings.cache_ttl_seconds,
            "entries": [
                {
                    "reference": key[0],
                    "tag": key[1],
                    "manifest_digest": entry.manifest_digest,
                    "opened": entry.brain is not None,
                    "age_seconds": round(now - entry.checked_at, 1)
                    if entry.checked_at
                    else None,
                    "failures": entry.failures,
                }
                for key, entry in list(self._entries.items())
            ],
        }

    # -- opening ------------------------------------------------------------- #

    async def _open(
        self,
        reference: str,
        tag: str,
        modules: list[MemoryType] | None,
        verification: VerificationPolicy,
    ) -> tuple[Brain, str]:
        """Pull *reference*:*tag* into a fresh brain; return it and the config digest."""
        brain = self._brain_factory()
        # Pinned: the fetch, the judgement of what was fetched and the pull
        # that installs it all see one manifest even if the tag moves.
        client = self._client_factory(set(modules) if modules else None, pinned=True)
        manifest = await client.resolve(reference, tag)
        fetched = await brain.fetch(client, reference, tag, modules=modules)
        # The SDK installs an UNAUTHORIZED head (signed by a key its own trust
        # root does not list) with a warning when nothing is pinned. A forged
        # authority is the one case where that is worse than refusing.
        offered = brain.authenticate(
            fetched.digest, policy=verification, stance=SnapshotStance.HEAD
        )
        if offered.state is AuthorshipState.UNAUTHORIZED:
            offered.require_authorized()
        await brain.pull(
            client,
            reference,
            tag,
            modules=modules,
            ignore_vector_indices=not self._loads_vector_indices(manifest, modules),
            verification=verification,
        )
        # The config digest, not the snapshot: a module projection is published
        # under a config whose digest differs from its source snapshot.
        return brain, str(manifest.config.digest)

    @staticmethod
    def _loads_vector_indices(manifest: Any, modules: list[MemoryType] | None) -> bool:
        provider = get_index_provider()
        for memory_type in modules or list(MemoryType):
            layer = manifest.vector_index_for(memory_type)
            if layer is None:
                continue
            published = (layer.annotations or {}).get(ANNOTATION_EMBEDDING_MODEL)
            if not provider.accepts_vector_index(published):
                return False
        return True

    async def _refresh(
        self,
        entry: _Entry,
        reference: str,
        tag: str,
        modules: list[MemoryType] | None,
        verification: VerificationPolicy,
        *,
        strict: bool = False,
    ) -> Brain:
        """Re-pull into a fresh brain and swap it in; keep the held one on failure.

        Searches already running keep the old object until they finish rather
        than reading a store being rewritten.
        """
        assert entry.brain is not None
        try:
            brain, digest = await self._open(reference, tag, modules, verification)
        except Exception as exc:  # noqa: BLE001 - an installed brain outlives its registry
            if strict or refuses_held(entry.brain, verification):
                raise
            level = (
                "ERROR"
                if isinstance(exc, (AuthenticityError, BrainManifestRejected))
                else "WARNING"
            )
            logger.log(
                level,
                "Could not refresh brain {}:{}; serving the snapshot already held: {}",
                reference,
                tag,
                exc,
            )
            entry.checked_at = self._clock()
            entry.verification = verification
            return entry.brain
        previous = entry.manifest_digest
        self._install(entry, brain, digest, verification)
        if digest != previous:
            entry.signatures = frozenset()
        return brain

    async def _revalidate(
        self,
        entry: _Entry,
        reference: str,
        tag: str,
        modules: list[MemoryType] | None,
        verification: VerificationPolicy,
    ) -> Brain:
        """Resolve the manifest; re-pull only if its digest or signatures moved."""
        assert entry.brain is not None
        client = self._client_factory(set(modules) if modules else None)
        try:
            manifest = await client.resolve(reference, tag)
        except BrainManifestRejected as exc:
            # Not an outage: it will fail the same way until someone acts.
            logger.error(
                "Brain {}:{} is published in a form this build cannot install; "
                "serving the snapshot already held: {}",
                reference,
                tag,
                exc,
            )
            entry.checked_at = self._clock()
            return entry.brain
        except Exception as exc:  # noqa: BLE001 - staleness beats unavailability
            logger.warning(
                "Could not revalidate brain {}:{}: {}; serving the snapshot already held",
                reference,
                tag,
                exc,
            )
            entry.checked_at = self._clock()
            return entry.brain

        remote = str(manifest.config.digest)
        if remote == entry.manifest_digest:
            entry.checked_at = self._clock()
            if not await self._signatures_moved(
                entry, client, reference, manifest, verification
            ):
                return entry.brain
            logger.info(
                "Brain {}:{} gained signatures; re-judging its head", reference, tag
            )
        else:
            logger.info(
                "Brain {}:{} moved to {}; re-pulling", reference, tag, remote[:19]
            )
        return await self._refresh(entry, reference, tag, modules, verification)

    async def _signatures_moved(
        self,
        entry: _Entry,
        client: Any,
        reference: str,
        manifest: Any,
        verification: VerificationPolicy,
    ) -> bool:
        """Whether new signatures on an unchanged manifest warrant a re-pull.

        Only a governed brain whose held head is not yet authorized can gain
        anything from them.
        """
        assert entry.brain is not None
        if getattr(entry.brain, "trust_root", None) is None:
            return False
        try:
            listed = await client.referrers(
                reference, manifest.digest, SIGNATURE_MEDIA_TYPE
            )
        except Exception as exc:  # noqa: BLE001 - the held brain is still served
            logger.debug("Could not list signatures of {}: {}", reference, exc)
            return False
        current = frozenset(str(d.digest) for d in listed)
        if current == entry.signatures:
            return False
        entry.signatures = current
        try:
            state = entry.brain.authenticate(policy=verification).state
        except Exception:  # noqa: BLE001 - judged again by the pull instead
            return True
        return state is not AuthorshipState.AUTHORIZED

    # -- bookkeeping --------------------------------------------------------- #

    def _install(
        self,
        entry: _Entry,
        brain: Brain,
        digest: str | None,
        verification: VerificationPolicy,
    ) -> None:
        entry.brain = brain
        entry.manifest_digest = digest
        entry.checked_at = self._clock()
        entry.failures = 0
        entry.retry_after = 0.0
        entry.verification = verification

    def _record_failure(self, entry: _Entry) -> None:
        entry.failures += 1
        delay = min(
            self.settings.cache_failure_backoff_seconds * (2 ** (entry.failures - 1)),
            self.settings.cache_failure_backoff_max_seconds,
        )
        entry.retry_after = self._clock() + delay

    def _evict_if_needed(self, protect: Key) -> None:
        """Evict least-recently-checked entries down to the bound.

        *protect* is the entry about to be opened: its ``checked_at`` is zero,
        so without this it would be the first victim -- dropped from the map
        while its caller holds its lock -- and single-flight would silently
        stop working.
        """
        limit = self.settings.cache_max_brains
        if len(self._entries) <= limit:
            return
        victims = sorted(
            (item for item in self._entries.items() if item[0] != protect),
            key=lambda item: item[1].checked_at,
        )
        for key, _ in victims[: len(self._entries) - limit]:
            logger.info(
                "Brain cache: evicting {}:{} to stay within {}", key[0], key[1], limit
            )
            self._entries.pop(key, None)
