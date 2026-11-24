"""The working store: one OCI image layout per brain, on a volume.

Decision D1 (revised): this service owns the working copy of every brain it
writes. The registry is where brains are *distributed* -- pushed, pulled,
indexed -- not where they are edited.

Layout of ``GBMS_STORE_ROOT``::

    <root>/
      <sha256(reference)[:32]>/      one directory per brain
        brain.json                   reference, default tag, title, who created it
        layout/                      the OCI image layout pyboltzmann writes
        .write.lock                  advisory lock held for the duration of a write

The directory name is a hash so no reference can choose where it lands.

Concurrency (decision D11): one writer replica owns the volume. Every write to
a brain holds that brain's lock -- an ``asyncio.Lock`` for the coroutines of
this process and an ``flock`` on ``.write.lock`` so a second process that
mounts the same volume by mistake is refused rather than interleaved. Reads do
not lock: they open a separate handle pinned to a snapshot, and every snapshot
is immutable while the pointers that name them are replaced atomically.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import threading
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from boltzmann import Brain, MemoryType, OciDigest, Snapshot
from boltzmann.brain import HEAD_POINTER, REFS_POINTER, BrainState, RefTable
from boltzmann.branches import DEFAULT_BRANCH, tag_for
from boltzmann.identity.serialization import canonicalize, parse_json_strict
from boltzmann.store import OciLayoutStore
from loguru import logger

from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.lifecycle.errors import (
    NOT_FAST_FORWARD,
    NOT_FOUND,
    REFUSED_POLICY,
    USAGE,
    LifecycleError,
)
from gaussia_brains_mcp_server.lifecycle.identity import reader_actor
from gaussia_brains_mcp_server.repo import OCI_TAG_PATTERN
from gaussia_brains_mcp_server.store import IndexProvider, get_index_provider

META_FILE = "brain.json"
LAYOUT_DIR = "layout"
LOCK_FILE = ".write.lock"
BRANCH_TAG_PREFIX = "br."


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class BrainMeta:
    """What the service records about a brain beside its layout."""

    reference: str
    default_tag: str = "latest"
    title: str = ""
    description: str = ""
    created_at: str = field(default_factory=_now)
    created_by: str = ""
    imported_from: str | None = None

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> BrainMeta:
        known = {k: data[k] for k in cls.__dataclass_fields__ if k in data}
        return cls(**known)


def validate_default_tag(tag: str) -> str:
    if not OCI_TAG_PATTERN.match(tag):
        raise LifecycleError(USAGE, f"{tag!r} is not a valid OCI tag")
    if tag.startswith(BRANCH_TAG_PREFIX):
        raise LifecycleError(
            USAGE,
            f"the default tag {tag!r} is in the branch namespace ({BRANCH_TAG_PREFIX}*); "
            f"main would publish where branches do",
        )
    return tag


def tag_for_branch(meta: BrainMeta, name: str) -> str:
    """The tag branch *name* publishes to: main's is the brain's default tag."""
    return meta.default_tag if name == DEFAULT_BRANCH else tag_for(name)


@dataclass(frozen=True)
class BranchHead:
    name: str
    tag: str
    snapshot: OciDigest
    current: bool
    #: Where the branch's own tag pointed when this brain last pushed or pulled it.
    published: OciDigest | None


@dataclass(frozen=True)
class Heads:
    """Every branch head, read off the pointers without opening the brain."""

    current: str
    head: OciDigest | None
    branches: dict[str, BranchHead]
    origin: Any | None = None

    def snapshot_of(self, branch: str) -> OciDigest:
        found = self.branches.get(branch)
        if found is None:
            raise LifecycleError(NOT_FOUND, f"no branch named {branch!r}")
        return found.snapshot


def _write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    scratch = path.with_name(f".tmp-{path.name}")
    scratch.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.replace(scratch, path)


class _ReadHandle:
    """A brain opened read-only at one snapshot, serialized by a thread lock."""

    def __init__(self, brain: Brain) -> None:
        self.brain = brain
        self.lock = threading.Lock()
        self.cache: dict[str, Any] = {}


class BrainHandle:
    """One brain in the working store."""

    def __init__(
        self,
        directory: Path,
        meta: BrainMeta,
        settings: Settings,
        provider: Callable[[], IndexProvider],
    ) -> None:
        self.directory = directory
        self.meta = meta
        self.settings = settings
        self._provider = provider
        self._lock = asyncio.Lock()
        self._writer: Brain | None = None
        self._readers: OrderedDict[str, _ReadHandle] = OrderedDict()
        self._readers_lock = threading.Lock()
        #: Who holds the write lock and doing what, for a refusal message.
        self.busy_with: str | None = None

    # -- paths ----------------------------------------------------------------- #

    @property
    def reference(self) -> str:
        return self.meta.reference

    @property
    def layout(self) -> Path:
        return self.directory / LAYOUT_DIR

    def store(self) -> OciLayoutStore:
        return OciLayoutStore(self.layout, create=True)

    def save_meta(self) -> None:
        _write_json_atomic(self.directory / META_FILE, self.meta.to_json())

    # -- pointers (no lock, no brain) ------------------------------------------ #

    def heads(self) -> Heads:
        """Branch heads as the pointers state them.

        The head pointer is the truth for the current branch; the ref table for
        the others. An interrupted checkout is resolved the way the SDK
        resolves it on its next open, without repairing anything.
        """
        store = self.store()
        raw_head = store.read_pointer(HEAD_POINTER)
        if not raw_head:
            return Heads(current=DEFAULT_BRANCH, head=None, branches={})
        state = BrainState.model_validate(parse_json_strict(raw_head))
        raw_refs = store.read_pointer(REFS_POINTER)
        table = (
            RefTable.model_validate(parse_json_strict(raw_refs)) if raw_refs else None
        )
        if table is None or not table.branches:
            published = state.origin.snapshot if state.origin is not None else None
            main = BranchHead(
                DEFAULT_BRANCH,
                tag_for_branch(self.meta, DEFAULT_BRANCH),
                state.snapshot,
                True,
                published
                if self._origin_is_own(state.origin, DEFAULT_BRANCH)
                else None,
            )
            return Heads(
                DEFAULT_BRANCH, state.snapshot, {DEFAULT_BRANCH: main}, state.origin
            )
        current = table.current
        if table.switching is not None:
            target = table.branches.get(table.switching)
            if target is not None and target.snapshot == state.snapshot:
                current = table.switching
        branches: dict[str, BranchHead] = {}
        for name, ref in table.branches.items():
            is_current = name == current
            origin = state.origin if is_current else ref.origin
            branches[name] = BranchHead(
                name=name,
                tag=tag_for_branch(self.meta, name),
                snapshot=state.snapshot if is_current else ref.snapshot,
                current=is_current,
                published=origin.snapshot
                if self._origin_is_own(origin, name)
                else None,
            )
        return Heads(current, state.snapshot, branches, state.origin)

    def _origin_is_own(self, origin: Any, branch: str) -> bool:
        """An origin counts as the branch's publication only at its own tag here."""
        return (
            origin is not None
            and origin.reference == self.reference
            and origin.tag == tag_for_branch(self.meta, branch)
        )

    def snapshot(self, digest: OciDigest) -> Snapshot:
        return Snapshot.from_document(self.store().get_bytes(digest))

    # -- reading --------------------------------------------------------------- #

    @contextlib.contextmanager
    def reading(self, snapshot: OciDigest) -> Any:
        """A read handle pinned to *snapshot*, exclusive for the duration.

        Opening a brain rebuilds its structural indices, so handles are kept
        (a few per brain, least recently used dropped first).
        """
        key = str(snapshot)
        with self._readers_lock:
            handle = self._readers.get(key)
            if handle is not None:
                self._readers.move_to_end(key)
        if handle is None:
            handle = _ReadHandle(self._open(snapshot=self.snapshot(snapshot)))
            with self._readers_lock:
                self._readers[key] = handle
                while len(self._readers) > self.settings.store_max_read_handles:
                    self._readers.popitem(last=False)
        with handle.lock:
            yield handle

    def _open(self, snapshot: Snapshot | None = None) -> Brain:
        provider = self._provider()
        indices = provider.indices()
        return Brain(
            self.store(),
            actor=reader_actor(),
            snapshot=snapshot,
            planner=provider.planner(indices),
            indices=indices,
        )

    # -- writing --------------------------------------------------------------- #

    @contextlib.asynccontextmanager
    async def writing(self, what: str) -> AsyncIterator[None]:
        """Hold this brain's write lock (in-process and on the volume)."""
        timeout = self.settings.store_lock_timeout_seconds
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
        except TimeoutError as exc:
            raise LifecycleError(
                NOT_FAST_FORWARD,
                f"brain {self.reference} is busy ({self.busy_with or 'another write'}); "
                f"retry when it finishes",
                detail={"busy_with": self.busy_with},
            ) from exc
        lock_file = None
        try:
            lock_file = open(self.directory / LOCK_FILE, "a+")  # noqa: SIM115
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LifecycleError(
                    REFUSED_POLICY,
                    f"another process holds the write lock of {self.reference}; "
                    f"only one writer replica may mount the working store",
                ) from exc
            self.busy_with = what
            yield
        finally:
            self.busy_with = None
            if lock_file is not None:
                with contextlib.suppress(OSError):
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
            self._lock.release()

    def writer(self) -> Brain:
        """The writable handle. Only to be used while holding :meth:`writing`."""
        if self._writer is None:
            self._writer = self._open()
        return self._writer

    def reset_writer(self) -> None:
        """Drop the writable handle, so the next write reopens from disk."""
        self._writer = None

    def align_default_branch(self, brain: Brain) -> None:
        """Make ``main`` publish to the brain's default tag in its ref table.

        The SDK records ``latest`` for main when it first writes the ref table;
        a brain whose default tag is ``stable`` would otherwise route a pull of
        ``stable`` into the current branch instead of into ``main``.
        """
        raw = brain.store.read_pointer(REFS_POINTER)
        if not raw:
            return
        table = RefTable.model_validate(parse_json_strict(raw))
        main = table.branches.get(DEFAULT_BRANCH)
        wanted = self.meta.default_tag
        if main is None or (main.tag == wanted and main.reference == self.reference):
            return
        aligned = table.model_copy(
            update={
                "branches": {
                    **table.branches,
                    DEFAULT_BRANCH: main.model_copy(
                        update={"tag": wanted, "reference": self.reference}
                    ),
                }
            }
        )
        brain.store.write_pointer(
            REFS_POINTER,
            canonicalize(aligned.model_dump(mode="json", exclude_none=True)),
        )

    def forget_readers(self) -> None:
        with self._readers_lock:
            self._readers.clear()


def brain_dir_name(reference: str) -> str:
    return hashlib.sha256(reference.encode()).hexdigest()[:32]


class WorkingStore:
    """Every brain this replica writes."""

    def __init__(
        self,
        settings: Settings,
        *,
        provider: Callable[[], IndexProvider] = get_index_provider,
    ) -> None:
        if not settings.store_root:
            raise ValueError("GBMS_STORE_ROOT is not set")
        self.settings = settings
        self.root = Path(settings.store_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._provider = provider
        self._handles: dict[str, BrainHandle] = {}
        self._lock = threading.Lock()

    def _directory(self, reference: str) -> Path:
        return self.root / brain_dir_name(reference)

    def exists(self, reference: str) -> bool:
        return (self._directory(reference) / META_FILE).is_file()

    def get(self, reference: str) -> BrainHandle:
        with self._lock:
            handle = self._handles.get(reference)
            if handle is not None:
                return handle
            directory = self._directory(reference)
            meta_path = directory / META_FILE
            if not meta_path.is_file():
                raise LifecycleError(
                    NOT_FOUND, f"brain {reference} is not held by this service"
                )
            meta = BrainMeta.from_json(json.loads(meta_path.read_text()))
            if meta.reference != reference:
                raise LifecycleError(
                    NOT_FOUND, f"brain {reference} is not held by this service"
                )
            handle = BrainHandle(directory, meta, self.settings, self._provider)
            self._handles[reference] = handle
            return handle

    def find(self, reference: str) -> BrainHandle | None:
        try:
            return self.get(reference)
        except LifecycleError:
            return None

    def create(self, meta: BrainMeta) -> BrainHandle:
        validate_default_tag(meta.default_tag)
        with self._lock:
            directory = self._directory(meta.reference)
            if (directory / META_FILE).is_file():
                raise LifecycleError(
                    USAGE,
                    f"brain {meta.reference} already exists",
                    detail={"exists": True},
                )
            (directory / LAYOUT_DIR).mkdir(parents=True, exist_ok=True)
            OciLayoutStore(directory / LAYOUT_DIR, create=True)
            _write_json_atomic(directory / META_FILE, meta.to_json())
            handle = BrainHandle(directory, meta, self.settings, self._provider)
            self._handles[meta.reference] = handle
        logger.info("Working store: created {}", meta.reference)
        return handle

    def discard(self, reference: str) -> None:
        """Remove a brain that never got a history (a failed import)."""
        import shutil

        with self._lock:
            self._handles.pop(reference, None)
            directory = self._directory(reference)
            if directory.is_dir():
                shutil.rmtree(directory)

    def list(self) -> list[BrainMeta]:
        found: list[BrainMeta] = []
        for meta_path in sorted(self.root.glob(f"*/{META_FILE}")):
            try:
                found.append(BrainMeta.from_json(json.loads(meta_path.read_text())))
            except (OSError, ValueError) as exc:
                logger.warning("Working store: unreadable {}: {}", meta_path, exc)
        return found


def memory_types(values: list[str] | None) -> list[MemoryType] | None:
    if values is None:
        return None
    try:
        return [MemoryType(v) for v in values]
    except ValueError as exc:
        raise LifecycleError(USAGE, f"unknown memory type: {exc}") from exc
