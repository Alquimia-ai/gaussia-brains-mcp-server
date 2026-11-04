"""Read-only OCI registry client for Boltzmann Brain artifacts.

Implements the subset of pyboltzmann's ``RegistryClient`` protocol the read
path uses -- ``resolve``, ``list_tags``, ``pull_blob``, ``referrers`` -- by
shelling out to the ``oras`` CLI. Snapshot authorship is judged by pyboltzmann
(``Brain.authenticate`` / ``Brain.pull``); publisher signatures (cosign) are an
independent, optional gate applied at manifest resolution.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from boltzmann import MemoryType, OciDigest
from boltzmann.distribution import (
    BrainManifest,
    parse_manifest,
    require_supported_schemas,
)
from boltzmann.distribution.manifest import Descriptor
from boltzmann.distribution.media_types import (
    ARTIFACT_TYPE,
    CONFIG_MEDIA_TYPE,
    HISTORY_MEDIA_TYPE,
    MODULE_MEDIA_TYPE_TEMPLATE,
    PROJECTION_MEDIA_TYPE,
    VECTOR_INDEX_MEDIA_TYPE,
)
from boltzmann.exceptions import DistributionError, ReferenceNotFoundError
from boltzmann.store import BlockStore
from loguru import logger

from gaussia_brains_mcp_server.config import Settings, get_settings
from gaussia_brains_mcp_server.signatures import get_signature_verifier

BOLTZMANN_LAYER_MEDIA_TYPES = frozenset(
    {
        CONFIG_MEDIA_TYPE,
        PROJECTION_MEDIA_TYPE,
        HISTORY_MEDIA_TYPE,
        VECTOR_INDEX_MEDIA_TYPE,
        *(MODULE_MEDIA_TYPE_TEMPLATE.format(memory_type=mt.value) for mt in MemoryType),
    }
)


class BrainManifestRejected(DistributionError):
    """A manifest was fetched but is not a usable Boltzmann brain."""


def _names_a_missing_repository(detail: str) -> bool:
    lowered = detail.lower()
    return any(
        marker in lowered
        for marker in (
            "name_unknown",
            "name unknown",
            "not found",
            "404",
            "repository name not known",
        )
    )


def validate_manifest_document(document: dict[str, Any], target: str) -> None:
    """Refuse anything that is not a Boltzmann brain before parsing it."""
    artifact_type = document.get("artifactType")
    if artifact_type != ARTIFACT_TYPE:
        raise BrainManifestRejected(
            f"{target} is not a Boltzmann brain: artifactType is {artifact_type!r}"
        )
    layers = document.get("layers") or []
    if not layers:
        raise BrainManifestRejected(f"{target} has no layers")
    for layer in layers:
        media_type = layer.get("mediaType")
        if media_type and media_type not in BOLTZMANN_LAYER_MEDIA_TYPES:
            raise BrainManifestRejected(
                f"{target} layer media type {media_type!r} is not part of the brain profile"
            )


def is_digest(tag: str) -> bool:
    return tag.startswith("sha256:")


def reference_target(reference: str, tag: str) -> str:
    """``repo:tag`` or, for a pinned digest, ``repo@sha256:...``."""
    return f"{reference}@{tag}" if is_digest(tag) else f"{reference}:{tag}"


def manifest_digest(raw: bytes) -> str:
    """The OCI digest of a manifest: sha256 over the bytes as served."""
    return "sha256:" + hashlib.sha256(raw).hexdigest()


class Oras:
    """Builds and runs ``oras`` invocations from :class:`Settings`."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def argv(self, *args: str) -> list[str]:
        # Transport and auth flags are sub-command flags: they go after the
        # sub-command (`oras --plain-http manifest fetch` is "unknown flag").
        s = self.settings
        flags: list[str] = []
        if s.oras_insecure:
            flags.append("--insecure")
        if s.oras_plain_http:
            flags.append("--plain-http")
        if s.registry_config:
            flags += ["--registry-config", s.registry_config]
        if s.registry_username and s.registry_password:
            # Via stdin, never argv: argv is visible to every process on the host.
            flags += ["--username", s.registry_username, "--password-stdin"]
        return [s.oras_bin, *args, *flags]

    def _stdin(self) -> str | None:
        s = self.settings
        if s.registry_username and s.registry_password:
            return s.registry_password.get_secret_value()
        return None

    async def run(self, *args: str, what: str) -> str:
        """Run ``oras <args>`` and return stdout as text."""
        return (await self.run_bytes(*args, what=what)).decode()

    async def run_bytes(self, *args: str, what: str) -> bytes:
        """Run ``oras <args>`` and return stdout exactly as emitted.

        Bytes, not text: a manifest's digest is the hash of the bytes the
        registry served, and text decoding may rewrite line endings.
        """
        timeout = self.settings.oras_timeout_seconds
        password = self._stdin()
        try:
            completed = await asyncio.to_thread(
                subprocess.run,
                self.argv(*args),
                input=password.encode() if password is not None else None,
                capture_output=True,
                check=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise DistributionError(f"timed out after {timeout}s {what}") from exc
        except subprocess.CalledProcessError as exc:
            raw = exc.stderr or exc.stdout or b""
            detail = (raw.decode(errors="replace") if raw else str(exc)).strip()
            if _names_a_missing_repository(detail):
                raise ReferenceNotFoundError(f"{what}: {detail}") from exc
            raise DistributionError(f"failed {what}: {detail}") from exc
        except FileNotFoundError as exc:
            raise DistributionError(
                f"oras binary {self.settings.oras_bin!r} not found"
            ) from exc
        return completed.stdout


class GaussiaRegistryClient:
    """Read-only ``RegistryClient`` for Boltzmann brains."""

    def __init__(
        self,
        modules: set[MemoryType] | None = None,
        *,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.modules = modules
        self._oras = Oras(self.settings)
        self._signatures = get_signature_verifier(self.settings)
        self._pinned: dict[str, BrainManifest] | None = None
        #: Digest of the manifest most recently resolved by this client.
        self.last_digest: str | None = None

    def pinned(self) -> GaussiaRegistryClient:
        """A client that resolves each tag once and reuses that manifest.

        ``Brain.fetch`` and ``Brain.pull`` each resolve; pinning makes them
        judge and install the same manifest even if the tag moves in between.
        """
        twin = type(self)(self.modules, settings=self.settings)
        twin._pinned = {}
        return twin

    async def fetch_manifest(
        self, reference: str, tag: str
    ) -> tuple[dict[str, Any], str]:
        """Fetch a raw manifest document and its digest, with no profile checks.

        This is used for non-brain artifacts such as namespace indexes.
        """
        target = reference_target(reference, tag)
        raw = await self._oras.run_bytes(
            "manifest", "fetch", target, what=f"resolving {target}"
        )
        if not raw.strip():
            raise ReferenceNotFoundError(f"no manifest found for {target}")
        digest = manifest_digest(raw)
        try:
            document = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BrainManifestRejected(
                f"{target} manifest is not JSON: {exc}"
            ) from exc
        return document, digest

    async def resolve(self, reference: str, tag: str) -> BrainManifest:
        """Fetch and validate a manifest, downloading no blobs.

        *tag* may be a tag or a pinned digest (``sha256:...``); pyboltzmann
        passes either through unchanged. The publisher-signature gate runs on
        the digest of the bytes actually served, so a tag that moves between
        the check and the pull cannot swap what was checked.
        """
        target = reference_target(reference, tag)
        if self._pinned is not None and target in self._pinned:
            return self._pinned[target]

        raw = await self._oras.run_bytes(
            "manifest", "fetch", target, what=f"resolving {target}"
        )
        if not raw.strip():
            raise ReferenceNotFoundError(f"no manifest found for {target}")
        digest = manifest_digest(raw)
        if is_digest(tag) and digest != tag:
            raise BrainManifestRejected(
                f"{target} served a manifest hashing to {digest}"
            )
        try:
            document = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BrainManifestRejected(
                f"{target} manifest is not JSON: {exc}"
            ) from exc
        validate_manifest_document(document, target)

        manifest = parse_manifest(raw)
        if self.modules:
            require_supported_schemas(manifest, self.modules)
        await asyncio.to_thread(self._signatures.gate, f"{reference}@{digest}")

        self.last_digest = digest
        if self._pinned is not None:
            self._pinned[target] = manifest
        return manifest

    async def list_tags(self, reference: str) -> list[str]:
        """Every tag held by *reference* (unfiltered; ``oras`` follows pagination)."""
        raw = await self._oras.run(
            "repo",
            "tags",
            reference,
            "--format",
            "json",
            what=f"listing the tags of {reference}",
        )
        if not raw.strip():
            return []
        return list(json.loads(raw).get("tags") or [])

    async def pull_blob(
        self, reference: str, digest: OciDigest, store: BlockStore
    ) -> None:
        """Download one blob into *store*, verifying its digest on arrival."""
        target = f"{reference}@{digest}"
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "blob"
            await self._oras.run(
                "blob",
                "fetch",
                target,
                "--output",
                str(out),
                what=f"fetching blob {target}",
            )
            data = await asyncio.to_thread(out.read_bytes)
        written = store.put_bytes(data)
        if written != digest:
            raise DistributionError(
                f"blob digest mismatch pulling {digest} from {reference}: "
                f"registry returned bytes hashing to {written}"
            )

    async def referrers(
        self, reference: str, subject: OciDigest, artifact_type: str | None = None
    ) -> list[Descriptor]:
        """Manifests referring to *subject* (e.g. signatures), filtered by type.

        A referrer listing is shared space, so entries are parsed only once
        they claim the requested type and one unreadable entry hides nothing.
        """
        args = ["discover", f"{reference}@{subject}", "--format", "json"]
        if artifact_type is not None:
            args += ["--artifact-type", artifact_type]
        raw = await self._oras.run(
            *args, what=f"listing referrers of {reference}@{subject}"
        )
        try:
            listing = json.loads(raw) if raw.strip() else []
        except json.JSONDecodeError as exc:
            raise DistributionError(
                f"referrer listing of {subject} is not JSON: {exc}"
            ) from exc
        # ORAS >= 1.2 reports "referrers"; older builds emit an index with "manifests".
        entries = (
            listing
            if isinstance(listing, list)
            else listing.get("referrers") or listing.get("manifests") or []
        )
        found: list[Descriptor] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if artifact_type is not None and entry.get("artifactType") != artifact_type:
                continue
            fields = ("mediaType", "digest", "size", "annotations", "artifactType")
            try:
                found.append(
                    Descriptor.model_validate(
                        {k: entry[k] for k in fields if entry.get(k) is not None}
                    )
                )
            except ValueError:
                logger.warning("Skipping an unreadable referrer of {}", subject)
        return found


def registry_client(
    modules: set[MemoryType] | None = None, *, pinned: bool = False
) -> GaussiaRegistryClient:
    """Factory for a :class:`GaussiaRegistryClient`."""
    client = GaussiaRegistryClient(modules)
    return client.pinned() if pinned else client
