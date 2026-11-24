"""OCI registry client for Boltzmann Brain artifacts.

Implements pyboltzmann's ``RegistryClient`` protocol -- ``resolve``,
``list_tags``, ``pull_blob``, ``referrers`` for reads; ``push``,
``push_referrer``, ``pull_referrer`` for the working store's distribution --
plus the namespace-index writes needed to publish, unpublish and reindex
brains, all by shelling out to the ``oras`` CLI with server-held credentials. Snapshot authorship is judged by
pyboltzmann (``Brain.authenticate`` / ``Brain.pull``); publisher signatures
(cosign) are an independent, optional gate applied at manifest resolution.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
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

#: OCI artifact type for the namespace index anchor manifest.
INDEX_ARTIFACT_TYPE = "application/vnd.alquimia.registry.index.v1"
#: OCI artifact type for an index entry referrer.
ENTRY_ARTIFACT_TYPE = "application/vnd.alquimia.registry.index.entry.v1"
#: Tag the namespace index anchor is served under.
INDEX_TAG = "v1"
#: Schema annotation stamped on every index entry.
_ANNOTATION_PREFIX = "ai.alquimia.registry.index"
#: Empty config blob digest required by OCI image manifests.
_OCI_EMPTY_BLOB = b"{}"
_OCI_EMPTY_BLOB_DIGEST = (
    "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
)
_OCI_EMPTY_CONFIG_MEDIA_TYPE = "application/vnd.oci.empty.v1+json"

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


def _annotation(key: str) -> str:
    return f"{_ANNOTATION_PREFIX}.{key}"


def repo_path(namespace: str, repo: str) -> str:
    """*repo* without the registry host of *namespace* (``brains/x``).

    Index entries name repositories relative to the registry; accept a fully
    qualified reference too, so both spellings address the same entry.
    """
    host = namespace.partition("/")[0]
    prefix = f"{host}/"
    return repo[len(prefix) :] if host and repo.startswith(prefix) else repo


def _entry_blob_bytes(
    repo: str,
    tag: str,
    digest: str,
    kind: str = "brain",
    description: str = "",
    author: str = "",
    published_at: str = "",
    extra: dict[str, str] | None = None,
    modules: list[str] | None = None,
) -> bytes:
    """Return canonical bytes for an index entry payload layer."""
    payload: dict[str, Any] = {
        "schema": "https://alquimia.ai/schema/index-entry+v1",
        "version": "v1",
        "repo": repo,
        "tag": tag,
        "digest": digest,
        "kind": kind,
    }
    if description:
        payload["description"] = description
    if author:
        payload["author"] = author
    if published_at:
        payload["published_at"] = published_at
    if modules:
        payload["modules"] = list(modules)
    if extra:
        payload["annotations"] = extra
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _entry_annotations(
    repo: str,
    tag: str,
    digest: str,
    kind: str = "brain",
    description: str = "",
    author: str = "",
    published_at: str = "",
    extra: dict[str, str] | None = None,
    modules: list[str] | None = None,
) -> dict[str, str]:
    """Return the annotation set stamped on an index entry artifact."""
    annotations: dict[str, str] = {
        _annotation("schema"): "https://alquimia.ai/schema/index-entry+v1",
        _annotation("version"): "v1",
        _annotation("repo"): repo,
        _annotation("tag"): tag,
        _annotation("digest"): digest,
        _annotation("kind"): kind,
    }
    if description:
        annotations[_annotation("description")] = description
    if author:
        annotations[_annotation("author")] = author
    if published_at:
        annotations[_annotation("published-at")] = published_at
    if modules:
        annotations[_annotation("modules")] = ",".join(modules)
    if extra:
        annotations.update(extra)
    return annotations


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

    async def run(self, *args: str, what: str, cwd: str | Path | None = None) -> str:
        """Run ``oras <args>`` and return stdout as text."""
        return (await self.run_bytes(*args, what=what, cwd=cwd)).decode()

    async def run_bytes(
        self, *args: str, what: str, cwd: str | Path | None = None
    ) -> bytes:
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
                cwd=str(cwd) if cwd is not None else None,
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

    async def manifest_modules(self, reference: str, digest: str) -> list[str]:
        """Memory modules of the brain at ``reference@digest``.

        A cheap read for listings: the manifest is checked against its digest
        and the brain profile, but no publisher-signature gate runs (callers
        report signatures separately).
        """
        target = reference_target(reference, digest)
        raw = await self._oras.run_bytes(
            "manifest", "fetch", target, what=f"resolving {target}"
        )
        if not raw.strip():
            raise ReferenceNotFoundError(f"no manifest found for {target}")
        served = manifest_digest(raw)
        if is_digest(digest) and served != digest:
            raise BrainManifestRejected(
                f"{target} served a manifest hashing to {served}"
            )
        try:
            document = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BrainManifestRejected(
                f"{target} manifest is not JSON: {exc}"
            ) from exc
        validate_manifest_document(document, target)
        return [mt.value for mt in parse_manifest(raw).modules]

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
        return await self._discover(f"{reference}@{subject}", artifact_type)

    async def _discover(
        self, target: str, artifact_type: str | None
    ) -> list[Descriptor]:
        """``oras discover`` on *target* (a tag or a digest reference)."""
        args = ["discover", target, "--format", "json"]
        if artifact_type is not None:
            args += ["--artifact-type", artifact_type]
        raw = await self._oras.run(*args, what=f"listing referrers of {target}")
        try:
            listing = json.loads(raw) if raw.strip() else []
        except json.JSONDecodeError as exc:
            raise DistributionError(
                f"referrer listing of {target} is not JSON: {exc}"
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
                logger.warning("Skipping an unreadable referrer of {}", target)
        return found

    # -- Publishing (the write path) ----------------------------------------- #

    async def push(
        self,
        reference: str,
        tag: str,
        manifest: BrainManifest,
        store: BlockStore,
    ) -> OciDigest:
        """Publish an already-packed artifact: every blob, then the manifest.

        The fast-forward check lives in ``Brain.push`` and is not repeated
        here. Inside a job, the manifest write is the point of no return: a
        cancel is honoured up to the last blob and refused from then on.
        """
        from gaussia_brains_mcp_server.lifecycle.jobs import current_job

        target = reference_target(reference, tag)
        descriptors = [manifest.config, *manifest.layers]
        for descriptor in descriptors:
            if descriptor.media_type not in BOLTZMANN_LAYER_MEDIA_TYPES:
                raise DistributionError(
                    f"refusing to push a layer of media type {descriptor.media_type!r}: "
                    f"not part of the brain profile"
                )
        job = current_job()
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for descriptor in descriptors:
                if job is not None:
                    job.checkpoint()
                blob = tmpdir / descriptor.digest.hex
                blob.write_bytes(store.get_bytes(descriptor.digest))
                await self._oras.run(
                    "blob",
                    "push",
                    f"{reference}@{descriptor.digest}",
                    str(blob),
                    what=f"pushing blob {descriptor.digest.short} to {reference}",
                )
            manifest_file = tmpdir / "manifest.json"
            manifest_file.write_bytes(manifest.to_bytes())
            if job is not None:
                job.commit_point(f"writing manifest {target}")
            await self._oras.run(
                "manifest",
                "push",
                target,
                str(manifest_file),
                "--media-type",
                manifest.media_type,
                what=f"pushing manifest {target}",
            )
        return manifest.digest

    async def push_referrer(
        self, reference: str, manifest: Any, store: BlockStore
    ) -> OciDigest:
        """Publish a signature manifest referring to a brain already pushed.

        Brains created here are ungoverned and carry no signatures; this is
        what lets an imported governed brain keep its signatures on a push.
        """
        from boltzmann.distribution.media_types import (
            EMPTY_CONFIG_BYTES,
            MANIFEST_MEDIA_TYPE,
        )

        payload = manifest.to_bytes()
        digest = OciDigest.of(payload)
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for descriptor, data in (
                (manifest.config, EMPTY_CONFIG_BYTES),
                (manifest.record, store.get_bytes(manifest.record.digest)),
            ):
                blob = tmpdir / descriptor.digest.hex
                blob.write_bytes(data)
                await self._oras.run(
                    "blob",
                    "push",
                    f"{reference}@{descriptor.digest}",
                    str(blob),
                    what=f"pushing signature blob {descriptor.digest.short}",
                )
            manifest_file = tmpdir / "signature.json"
            manifest_file.write_bytes(payload)
            await self._oras.run(
                "manifest",
                "push",
                f"{reference}@{digest}",
                str(manifest_file),
                "--media-type",
                MANIFEST_MEDIA_TYPE,
                what=f"pushing signature manifest {digest.short}",
            )
        return digest

    async def pull_referrer(self, reference: str, digest: OciDigest) -> Any:
        """Fetch one signature manifest by digest."""
        from boltzmann.distribution.manifest import parse_signature_manifest

        target = f"{reference}@{digest}"
        raw = await self._oras.run_bytes(
            "manifest", "fetch", target, what=f"fetching {target}"
        )
        return parse_signature_manifest(raw)

    # -- Namespace index management ------------------------------------------ #

    def _anchor_reference(self, namespace: str) -> str:
        return f"{namespace}/index:{INDEX_TAG}"

    async def index_entries(self, namespace: str) -> list[Descriptor]:
        """Every entry attached to the namespace index anchor, in one call.

        ``oras discover`` resolves the anchor tag itself, so the anchor
        manifest is not fetched separately. A namespace with no index yet has
        no entries.
        """
        try:
            return await self._discover(
                self._anchor_reference(namespace), ENTRY_ARTIFACT_TYPE
            )
        except ReferenceNotFoundError:
            return []

    async def ensure_index(self, namespace: str) -> str:
        """Ensure the namespace index anchor exists and return its tag reference."""
        anchor = self._anchor_reference(namespace)
        repository = f"{namespace}/index"

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "empty.json"
            config_path.write_bytes(_OCI_EMPTY_BLOB)
            manifest_path = Path(tmp) / "anchor.json"
            manifest: dict[str, Any] = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "artifactType": INDEX_ARTIFACT_TYPE,
                "config": {
                    "mediaType": _OCI_EMPTY_CONFIG_MEDIA_TYPE,
                    "digest": _OCI_EMPTY_BLOB_DIGEST,
                    "size": len(_OCI_EMPTY_BLOB),
                },
                "layers": [],
                "annotations": {
                    "org.opencontainers.image.created": "1970-01-01T00:00:00Z"
                },
            }
            manifest_path.write_bytes(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            await self._oras.run(
                "blob",
                "push",
                repository,
                str(config_path),
                what=f"upload empty config to {repository}",
            )
            await self._oras.run(
                "manifest",
                "push",
                anchor,
                str(manifest_path),
                "--media-type",
                "application/vnd.oci.image.manifest.v1+json",
                what=f"ensure index anchor {anchor}",
            )
        return anchor

    async def attach_index_entry(
        self,
        namespace: str,
        repo: str,
        tag: str,
        digest: str,
        *,
        description: str = "",
        author: str = "",
        published_at: str = "",
        modules: list[str] | None = None,
        replace: bool = True,
    ) -> str | None:
        """Attach a ``kind=brain`` index entry for ``repo:tag`` to the namespace anchor.

        Returns the digest of the created entry artifact, if reported by ORAS.
        """
        repo = repo_path(namespace, repo)
        anchor = await self.ensure_index(namespace)
        if replace:
            await self.detach_index_entry(namespace, repo, tag, quiet=True)

        payload_bytes = _entry_blob_bytes(
            repo=repo,
            tag=tag,
            digest=digest,
            description=description,
            author=author,
            published_at=published_at,
            modules=modules,
        )
        with tempfile.TemporaryDirectory() as tmp:
            payload_path = Path(tmp) / "entry.json"
            payload_path.write_bytes(payload_bytes)
            args = [
                "attach",
                "--artifact-type",
                ENTRY_ARTIFACT_TYPE,
                "--distribution-spec",
                "v1.1-referrers-api",
                anchor,
                f"entry.json:{ENTRY_ARTIFACT_TYPE}+json",
            ]
            for key, value in _entry_annotations(
                repo=repo,
                tag=tag,
                digest=digest,
                description=description,
                author=author,
                published_at=published_at,
                modules=modules,
            ).items():
                args += ["--annotation", f"{key}={value}"]
            stdout = await self._oras.run(
                *args,
                what=f"attach index entry to {anchor}",
                cwd=tmp,
            )

        matches = re.findall(r"sha256:[0-9a-f]{64}", (stdout or ""))
        return matches[-1] if matches else None

    async def detach_index_entry(
        self, namespace: str, repo: str, tag: str, *, quiet: bool = False
    ) -> int:
        """Detach whatever the index advertises at ``repo:tag``.

        Returns the number of entry manifests removed.
        """
        repo = repo_path(namespace, repo)
        removed = 0
        for descriptor in await self.index_entries(namespace):
            annotations = descriptor.annotations or {}
            if (
                repo_path(namespace, annotations.get(_annotation("repo"), "")) == repo
                and annotations.get(_annotation("tag")) == tag
            ):
                target = f"{namespace}/index@{descriptor.digest}"
                try:
                    await self._oras.run(
                        "manifest",
                        "delete",
                        "-f",
                        target,
                        what=f"detach index entry {target}",
                    )
                    removed += 1
                except DistributionError as exc:
                    if not quiet:
                        raise
                    logger.debug("Could not detach {}: {}", target, exc)
        return removed

    async def discover_index_entries(self, namespace: str) -> list[dict[str, Any]]:
        """Return descriptors for every index entry attached to the namespace anchor."""
        entries: list[dict[str, Any]] = []
        for descriptor in await self.index_entries(namespace):
            annotations = descriptor.annotations or {}
            if annotations.get(_annotation("kind")) != "brain":
                continue
            entries.append(
                {
                    "digest": str(descriptor.digest),
                    "repo": annotations.get(_annotation("repo"), ""),
                    "tag": annotations.get(_annotation("tag"), ""),
                    "entry_digest": str(descriptor.digest),
                    "description": annotations.get(_annotation("description"), ""),
                    "author": annotations.get(_annotation("author"), ""),
                    "published_at": annotations.get(_annotation("published-at")),
                    "subject_digest": annotations.get(_annotation("digest")),
                    "modules": annotations.get(_annotation("modules"), ""),
                }
            )
        return entries


def registry_client(
    modules: set[MemoryType] | None = None, *, pinned: bool = False
) -> GaussiaRegistryClient:
    """Factory for a :class:`GaussiaRegistryClient`."""
    client = GaussiaRegistryClient(modules)
    return client.pinned() if pinned else client
