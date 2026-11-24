"""The brain lifecycle: every operation the REST surface exposes, without HTTP.

Ported from ``alquimia.boltzmann`` (``workspace``, ``branches``,
``distribution``, ``reconcile``), with the CLI and the registration taken out
and three service concerns put in:

* **Branch-addressed writes.** A working copy has one current branch, but many
  people write to one brain. Every write names the branch it is for (``main``
  when omitted); the service checks it out under the write lock first.
* **Optimistic concurrency.** A write may carry ``If-Match: <head digest>``;
  if the branch head moved since the client read it, the write is refused with
  ``not_fast_forward`` before anything is written.
* **Read-only governance.** Signing and governance are out of scope for now
  (decision D10): a governed brain -- one whose history names a trust root --
  is served read-only, and every write to it is ``refused_policy``.

The pyboltzmann calls are synchronous and CPU-bound, so they run in a worker
thread while the brain's write lock is held. Reads never take the lock.
"""

from __future__ import annotations

import asyncio
import base64
import shutil
import tarfile
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from boltzmann import (
    Actor,
    BlockId,
    Brain,
    Collaborator,
    MemoryType,
    OciDigest,
    ReconcileStrategy,
    ResolutionKind,
    Snapshot,
)
from boltzmann.blocks import Producer, ProducerKind
from boltzmann.branches import DEFAULT_BRANCH, branch_for_tag
from boltzmann.exceptions import (
    BranchError,
    ReconciliationHaltedError,
    ReferenceNotFoundError,
)
from boltzmann.identity.principal import is_actor_id
from boltzmann.ingest import Candidate, CandidateSet, RegistrationRequest
from boltzmann.module import Ledger
from boltzmann.store import OciLayoutStore
from loguru import logger

from gaussia_brains_mcp_server.cache import BrainCache, default_verification
from gaussia_brains_mcp_server.client import registry_client
from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.guidance import content_to_text
from gaussia_brains_mcp_server.indices.engines import _block_text
from gaussia_brains_mcp_server.lifecycle.errors import (
    NEEDS_HUMAN,
    NOT_FAST_FORWARD,
    NOT_FOUND,
    REFUSED_POLICY,
    REFUSED_PROTOCOL,
    REJECTED_CANDIDATES,
    USAGE,
    LifecycleError,
)
from gaussia_brains_mcp_server.lifecycle.identity import reader_actor
from gaussia_brains_mcp_server.lifecycle.jobs import Job, JobContext, JobRunner
from gaussia_brains_mcp_server.lifecycle.workstore import (
    BrainHandle,
    BrainMeta,
    WorkingStore,
    tag_for_branch,
    validate_default_tag,
)
from gaussia_brains_mcp_server.repo import parse_brain_ref
from gaussia_brains_mcp_server.search import SearchInputError, search_brain

MAX_PAGE = 200


# --------------------------------------------------------------------------- #
# Serialization helpers
# --------------------------------------------------------------------------- #


def _digest(value: Any) -> str | None:
    return str(value) if value is not None else None


def _dump(model: Any) -> Any:
    """A pydantic model as JSON-ready data (SDK models are pydantic)."""
    if model is None:
        return None
    try:
        return model.model_dump(mode="json", exclude_none=True)
    except Exception:  # noqa: BLE001 - a field that does not serialize
        data = model.model_dump(mode="python", exclude_none=True)
        return _jsonable(data)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "model_dump"):
        return _dump(value)
    return str(value)


def _snapshot_view(snapshot: Snapshot) -> dict[str, Any]:
    return {
        "snapshot": str(snapshot.digest),
        "created_at": snapshot.created_at,
        "parents": [str(p) for p in snapshot.parents],
        "reconciliation": snapshot.is_reconciliation,
        "labels": snapshot.labels or {},
        "governed": snapshot.trust_root is not None,
        "modules": {
            mt.value: {"block_count": ref.block_count, "root": str(ref.root)}
            for mt, ref in sorted(snapshot.modules.items(), key=lambda i: i[0].value)
        },
    }


def _report_view(report: Any) -> dict[str, Any]:
    results = []
    for index, result in enumerate(report.results):
        block = result.block
        results.append(
            {
                "index": index,
                "status": result.status.value,
                "memory_type": result.candidate.memory_type.value,
                "block_id": str(block.block_id) if block is not None else None,
                "issues": [_dump(issue) for issue in result.issues],
                "conflicts_with": [str(b) for b in result.conflicts_with],
                "candidate": _dump(result.candidate),
            }
        )
    return {
        "clean": report.is_clean,
        "committable": len(report.committable),
        "results": results,
        "producer": _dump(report.producer),
        "task_id": report.task_id,
        "checks": list(report.checks),
    }


def _encode_cursor(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> str:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        return base64.urlsafe_b64decode(padded.encode()).decode()
    except Exception as exc:  # noqa: BLE001
        raise LifecycleError(USAGE, "the cursor is not one this server issued") from exc


def _parse_block_id(raw: str) -> BlockId:
    try:
        return BlockId.parse(raw if raw.startswith("sha256:") else f"sha256:{raw}")
    except Exception as exc:  # noqa: BLE001
        raise LifecycleError(USAGE, f"{raw!r} is not a block id") from exc


def _parse_digest(raw: str, what: str = "digest") -> OciDigest:
    try:
        return OciDigest.parse(raw)
    except Exception as exc:  # noqa: BLE001
        raise LifecycleError(USAGE, f"{raw!r} is not a {what}") from exc


def normalize_if_match(value: str | None) -> str | None:
    """``If-Match`` as a bare digest: quotes and a weak prefix are dropped."""
    if value is None:
        return None
    value = value.strip()
    if value.startswith("W/"):
        value = value[2:]
    return value.strip('"') or None


# --------------------------------------------------------------------------- #
# Provenance view of one snapshot
# --------------------------------------------------------------------------- #


class ProvenanceView:
    """What the provenance module says, indexed by the block it is about.

    Built once per snapshot (and cached on the read handle): it means decoding
    every provenance block.
    """

    def __init__(self, brain: Brain) -> None:
        modules = brain.modules()
        self.ledger = Ledger.of(modules)
        self.added_at: dict[BlockId, str] = {}
        self.records: dict[BlockId, list[dict[str, Any]]] = {}
        provenance = modules.get(MemoryType.PROVENANCE)
        if provenance is None:
            return
        for entry_id in provenance.block_ids:
            if not provenance.store.is_resolvable(entry_id):
                continue
            try:
                entry = provenance.get(entry_id)
            except Exception:  # noqa: BLE001 - unreadable entries are skipped
                continue
            record = getattr(entry, "record", None)
            if record is None:
                continue
            data = _dump(record)
            data["entry"] = str(entry_id)
            subjects = list(getattr(record, "blocks", None) or [])
            if getattr(record, "block", None) is not None:
                subjects.append(record.block)
            for subject in subjects:
                self.records.setdefault(subject, []).append(data)
                if data.get("record_type") in ("registration", "derivation"):
                    at = data.get("at")
                    if isinstance(at, str) and (
                        subject not in self.added_at or at < self.added_at[subject]
                    ):
                        self.added_at[subject] = at
        for rows in self.records.values():
            rows.sort(key=lambda r: (r.get("at") or "", r.get("entry") or ""))


def _provenance(handle: Any) -> ProvenanceView:
    view = handle.cache.get("provenance")
    if view is None:
        view = ProvenanceView(handle.brain)
        handle.cache["provenance"] = view
    return view


# --------------------------------------------------------------------------- #
# The service
# --------------------------------------------------------------------------- #


class LifecycleService:
    """Every lifecycle operation over the working store."""

    def __init__(
        self,
        settings: Settings,
        store: WorkingStore,
        jobs: JobRunner,
        *,
        cache: BrainCache | None = None,
        client_factory: Callable[..., Any] = registry_client,
        catalog_factory: Callable[[Settings], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.jobs = jobs
        self.cache = cache
        self._client_factory = client_factory
        if catalog_factory is None:
            from gaussia_brains_mcp_server.catalog import Catalog

            def catalog_factory(s: Settings) -> Any:
                return Catalog(s, client_factory=client_factory)

        self._catalog_factory = catalog_factory

    # -- references ------------------------------------------------------------ #

    def repository(self, brain_ref: str) -> tuple[str, str | None]:
        """``(registry/namespace/name, tag-or-None)`` for a brain reference."""
        try:
            parsed = parse_brain_ref(
                brain_ref,
                registry_default=self.settings.registry_default,
                namespace_default=self.settings.registry_namespace_brains,
            )
        except ValueError as exc:
            raise LifecycleError(USAGE, f"invalid brain reference: {exc}") from exc
        if parsed.digest:
            raise LifecycleError(
                USAGE, "a brain in the working store is named without a digest"
            )
        explicit = brain_ref.rsplit("/", 1)[-1]
        tag = parsed.tag if ":" in explicit else None
        return parsed.reference, tag

    def held(self, brain_ref: str) -> BrainHandle | None:
        reference, _ = self.repository(brain_ref)
        return self.store.find(reference)

    def handle(self, brain_ref: str) -> BrainHandle:
        reference, _ = self.repository(brain_ref)
        return self.store.get(reference)

    def _require_source(self, reference: str) -> None:
        if not self.settings.source_allowed(reference):
            raise LifecycleError(
                REFUSED_POLICY,
                f"{reference} is not in an allowed source "
                f"({', '.join(self.settings.effective_sources()) or 'none configured'})",
            )

    # -- create ---------------------------------------------------------------- #

    def create(
        self,
        brain_ref: str,
        *,
        actor: Actor,
        default_tag: str | None = None,
        title: str = "",
        description: str = "",
    ) -> dict[str, Any]:
        reference, tag = self.repository(brain_ref)
        self._require_source(reference)
        meta = BrainMeta(
            reference=reference,
            default_tag=validate_default_tag(default_tag or tag or "latest"),
            title=title,
            description=description,
            created_by=actor.id,
        )
        handle = self.store.create(meta)
        return self.status(handle)

    # -- status ---------------------------------------------------------------- #

    def status(self, handle: BrainHandle, branch: str | None = None) -> dict[str, Any]:
        """Everything a client shows about a brain, read off the pointers."""
        heads = handle.heads()
        chosen = branch or DEFAULT_BRANCH
        head = heads.branches.get(chosen)
        if branch is not None and head is None and heads.head is not None:
            raise LifecycleError(NOT_FOUND, f"no branch named {branch!r}")
        snapshot = handle.snapshot(head.snapshot) if head is not None else None
        store = handle.store()
        convergence = None
        raw = store.read_pointer("reconcile")
        if raw:
            from boltzmann.identity.serialization import parse_json_strict
            from boltzmann.reconcile import ReconcileState

            state = ReconcileState.model_validate(parse_json_strict(raw))
            convergence = {
                "open": True,
                "theirs": str(state.theirs),
                "strategy": state.strategy.value,
                "reason": state.reason,
                "actor": state.actor.id,
                "head": str(state.head),
                "branch": heads.current,
                "resolutions": len(state.resolutions),
            }
        main = heads.branches.get(DEFAULT_BRANCH)
        publication = None
        if main is not None and main.published is not None:
            publication = {
                "reference": handle.reference,
                "tag": main.tag,
                "snapshot": str(main.published),
                "up_to_date": main.published == main.snapshot,
            }
        mcp = self.settings.mcp_path.rstrip("/") or "/mcp"
        meta = handle.meta
        return {
            "reference": handle.reference,
            "held": True,
            "title": meta.title,
            "description": meta.description,
            "default_tag": meta.default_tag,
            "created_at": meta.created_at,
            "created_by": meta.created_by,
            "imported_from": meta.imported_from,
            "governed": bool(snapshot and snapshot.trust_root is not None),
            "read_only": bool(snapshot and snapshot.trust_root is not None),
            "branch": chosen,
            "head": str(head.snapshot) if head is not None else None,
            "current_branch": heads.current,
            "modules": _snapshot_view(snapshot)["modules"] if snapshot else {},
            "block_count": snapshot.block_count if snapshot else 0,
            "branches": [
                {
                    "name": b.name,
                    "tag": b.tag,
                    "snapshot": str(b.snapshot),
                    "current": b.current,
                    "published": _digest(b.published),
                    "unpublished": b.published != b.snapshot,
                }
                for b in sorted(
                    heads.branches.values(),
                    key=lambda b: (b.name != DEFAULT_BRANCH, b.name),
                )
            ],
            "convergence": convergence,
            "publication": publication,
            "mcp": {"follow": f"{mcp}/{handle.reference}:{meta.default_tag}"},
        }

    def head_of(self, handle: BrainHandle, branch: str | None) -> OciDigest:
        heads = handle.heads()
        if heads.head is None:
            raise LifecycleError(
                NOT_FOUND, f"brain {handle.reference} has no history yet"
            )
        return heads.snapshot_of(branch or DEFAULT_BRANCH)

    # -- browse ---------------------------------------------------------------- #

    def list_blocks(
        self,
        handle: BrainHandle,
        memory_type: str,
        *,
        branch: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        try:
            mt = MemoryType(memory_type)
        except ValueError as exc:
            raise LifecycleError(USAGE, f"unknown memory type {memory_type!r}") from exc
        limit = max(1, min(limit, MAX_PAGE))
        snapshot = self.head_of(handle, branch)
        with handle.reading(snapshot) as reader:
            brain = reader.brain
            if not brain.snapshot().has_module(mt):
                return {
                    "memory_type": mt.value,
                    "snapshot": str(snapshot),
                    "blocks": [],
                    "next_cursor": None,
                    "total": 0,
                }
            view = _provenance(reader)
            module = brain.module(mt)
            ordered = reader.cache.get(("order", mt))
            if ordered is None:
                # Newest first by when the block entered the brain; blocks the
                # ledger says nothing about (a partial install) go last.
                ordered = sorted(
                    module.block_ids,
                    key=lambda b: (view.added_at.get(b) or "", str(b)),
                    reverse=True,
                )
                ordered.sort(key=lambda b: b not in view.added_at)
                reader.cache[("order", mt)] = ordered
            start = 0
            if cursor:
                last = _decode_cursor(cursor)
                positions = {str(b): i for i, b in enumerate(ordered)}
                if last not in positions:
                    raise LifecycleError(
                        USAGE, "the cursor names a block no longer in this module"
                    )
                start = positions[last] + 1
            page = ordered[start : start + limit]
            blocks = [self._block_row(brain, module, view, bid) for bid in page]
        more = start + limit < len(ordered)
        return {
            "memory_type": mt.value,
            "snapshot": str(snapshot),
            "total": len(ordered),
            "blocks": blocks,
            "next_cursor": _encode_cursor(str(page[-1])) if more and page else None,
        }

    def _block_row(
        self, brain: Brain, module: Any, view: ProvenanceView, bid: BlockId
    ) -> dict[str, Any]:
        ledger = view.ledger
        row: dict[str, Any] = {
            "block_id": str(bid),
            "memory_type": module.memory_type.value,
            "added_at": view.added_at.get(bid),
            "superseded_by": _digest(ledger.superseded_by.get(bid)),
            "demoted": bid in ledger.demoted,
            "tombstoned": bid in module.tombstones,
            "readable": True,
            "summary": None,
            "payload": None,
        }
        try:
            block = module.get(bid)
        except Exception as exc:  # noqa: BLE001 - listed, flagged unreadable
            row["readable"] = False
            row["unreadable_reason"] = type(exc).__name__
            return row
        payload = block.payload()
        row["payload"] = payload
        text = content_to_text(payload) if isinstance(payload, dict) else str(payload)
        row["summary"] = text[:280]
        return row

    def block_detail(
        self, handle: BrainHandle, block_id: str, *, branch: str | None = None
    ) -> dict[str, Any]:
        bid = _parse_block_id(block_id)
        snapshot = self.head_of(handle, branch)
        with handle.reading(snapshot) as reader:
            brain = reader.brain
            view = _provenance(reader)
            ledger = view.ledger
            owner = None
            for mt, module in brain.modules().items():
                if bid in module:
                    owner = (mt, module)
                    break
            if owner is None:
                raise LifecycleError(
                    NOT_FOUND, f"block {bid.short} is not in snapshot {snapshot.short}"
                )
            mt, module = owner
            detail = self._block_row(brain, module, view, bid)
            verifies = {"membership": True, "hash": False, "inclusion_proof": False}
            if detail["readable"]:
                block = module.get(bid)
                verifies["hash"] = block.block_id == bid
                try:
                    proof = module.inclusion_proof(bid)
                    verifies["inclusion_proof"] = bool(proof.verify(module.root))
                except Exception:  # noqa: BLE001
                    verifies["inclusion_proof"] = False
                detail["text"] = _block_text(block, brain.store)
                detail["resolvable"] = brain.store.is_resolvable(bid)
            detail.update(
                {
                    "snapshot": str(snapshot),
                    "root": str(module.root),
                    "verifies": verifies,
                    "provenance": view.records.get(bid, []),
                    "supersedes": [
                        str(old)
                        for old, new in ledger.superseded_by.items()
                        if new == bid
                    ],
                    "competing_successors": [
                        str(b)
                        for b in sorted(ledger.competing.get(bid, set()), key=str)
                    ],
                    "evidence": [str(b) for b in ledger.evidence.get(bid, [])],
                    "dependents": [
                        str(b)
                        for b in sorted(ledger.dependents.get(bid, set()), key=str)
                    ],
                    "producer": _dump(ledger.producers.get(bid)),
                    "assisted_by": [_dump(c) for c in ledger.assistance.get(bid, [])],
                    "validation": _dump(ledger.validations.get(bid)),
                    "locator": ledger.locators.get(bid),
                    "removed": bid in ledger.removed,
                }
            )
            return detail

    def history(
        self,
        handle: BrainHandle,
        *,
        branch: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """The branch's first-parent chain, newest first."""
        limit = max(1, min(limit, MAX_PAGE))
        heads = handle.heads()
        if heads.head is None:
            return {
                "branch": branch or DEFAULT_BRANCH,
                "snapshots": [],
                "next_cursor": None,
            }
        chosen = branch or DEFAULT_BRANCH
        head = heads.branches.get(chosen)
        if head is None:
            raise LifecycleError(NOT_FOUND, f"no branch named {chosen!r}")
        store = handle.store()
        current: OciDigest | None = head.snapshot
        if cursor:
            current = _parse_digest(_decode_cursor(cursor), "cursor")
            # The cursor is the next snapshot to show; it must be on this chain.
        rows: list[dict[str, Any]] = []
        while current is not None and len(rows) < limit:
            if not store.is_resolvable(current):
                rows.append({"snapshot": str(current), "pruned": True})
                current = None
                break
            snap = Snapshot.from_document(store.get_bytes(current))
            row = _snapshot_view(snap)
            row["published"] = head.published == current
            rows.append(row)
            current = snap.first_parent
        return {
            "branch": chosen,
            "head": str(head.snapshot),
            "snapshots": rows,
            "next_cursor": _encode_cursor(str(current))
            if current is not None
            else None,
        }

    # -- search ---------------------------------------------------------------- #

    def search(
        self,
        handle: BrainHandle,
        query: str,
        *,
        branch: str | None = None,
        **params: Any,
    ) -> dict[str, Any]:
        snapshot = self.head_of(handle, branch)
        with handle.reading(snapshot) as reader:
            try:
                found = search_brain(reader.brain, query, **params)
            except SearchInputError as exc:
                raise LifecycleError(USAGE, str(exc)) from exc
        found["snapshot"] = str(snapshot)
        found["branch"] = branch or DEFAULT_BRANCH
        return found

    async def search_registry(
        self, brain_ref: str, query: str, **params: Any
    ) -> dict[str, Any]:
        if self.cache is None:
            raise LifecycleError(
                NOT_FOUND, f"brain {brain_ref} is not held by this service"
            )
        return await search_registry(
            self.settings, self.cache, brain_ref, query, **params
        )

    # -- the write path -------------------------------------------------------- #

    @staticmethod
    def _require_writable(brain: Brain) -> None:
        if brain.trust_root is not None:
            raise LifecycleError(
                REFUSED_POLICY,
                "this brain is governed (its history names a trust root) and is served "
                "read-only until signing is supported here",
                detail={"governed": True},
            )

    @staticmethod
    def _switch_to(handle: BrainHandle, brain: Brain, branch: str) -> None:
        """Make *branch* current in the working copy (writes go to the current one)."""
        held = {info.name for info in brain.branches()}
        if not held:
            if branch != DEFAULT_BRANCH:
                raise LifecycleError(
                    NOT_FOUND,
                    f"no branch named {branch!r}: the brain has no history yet, so only "
                    f"{DEFAULT_BRANCH!r} can be written",
                )
            return
        if branch not in held:
            raise LifecycleError(NOT_FOUND, f"no branch named {branch!r}")
        if brain.current_branch() != branch:
            brain.checkout(branch)
            handle.align_default_branch(brain)

    @staticmethod
    def _check_if_match(brain: Brain, if_match: str | None) -> None:
        wanted = normalize_if_match(if_match)
        if wanted is None:
            return
        state = brain.state()
        head = state.get("snapshot") if state else None
        if wanted == "*":
            if head is None:
                raise LifecycleError(NOT_FAST_FORWARD, "the branch has no head yet")
            return
        if head != wanted:
            raise LifecycleError(
                NOT_FAST_FORWARD,
                f"the branch head is {head or 'empty'}, not {wanted}; re-read and retry",
                detail={"head": head, "if_match": wanted},
            )

    async def _write(
        self,
        handle: BrainHandle,
        what: str,
        fn: Callable[[Brain], Any],
        *,
        actor: Actor,
        branch: str | None = None,
        if_match: str | None = None,
        assisted_by: list[Collaborator] | None = None,
        switch: bool = True,
    ) -> Any:
        async with handle.writing(what):

            def run() -> Any:
                brain = handle.writer()
                self._require_writable(brain)
                if switch:
                    self._switch_to(handle, brain, branch or DEFAULT_BRANCH)
                self._check_if_match(brain, if_match)
                brain.actor = actor
                brain.assisted_by = list(assisted_by or [])
                try:
                    return fn(brain)
                finally:
                    brain.actor = reader_actor()
                    brain.assisted_by = []
                    handle.align_default_branch(brain)

            try:
                return await asyncio.to_thread(run)
            except LifecycleError:
                raise
            except Exception:
                # The handle may hold state the failed call half-updated; the
                # next write reopens it from what is on disk.
                handle.reset_writer()
                raise

    def _head_after(self, brain: Brain) -> str | None:
        return brain.state().get("snapshot")

    # -- evidence and commits --------------------------------------------------- #

    async def register_evidence(
        self,
        handle: BrainHandle,
        data: bytes,
        *,
        actor: Actor,
        media_type: str,
        origin: str | None = None,
        license: str | None = None,
        branch: str | None = None,
        if_match: str | None = None,
    ) -> dict[str, Any]:
        if not data:
            raise LifecycleError(USAGE, "the evidence is empty")

        def op(brain: Brain) -> dict[str, Any]:
            result = brain.register(
                data,
                RegistrationRequest(
                    media_type=media_type, actor=actor, origin=origin, license=license
                ),
            )
            return {
                "block_id": str(result.block_id),
                "duplicate": result.duplicate,
                "snapshot": _digest(result.commit.snapshot.digest)
                if result.commit
                else None,
                "head": self._head_after(brain),
                "branch": branch or DEFAULT_BRANCH,
                "size": len(data),
                "media_type": media_type,
            }

        return await self._write(
            handle,
            "registering evidence",
            op,
            actor=actor,
            branch=branch,
            if_match=if_match,
        )

    async def commit(
        self,
        handle: BrainHandle,
        *,
        actor: Actor,
        source: str,
        candidates: list[dict[str, Any]],
        producer: dict[str, Any] | None,
        allowed: list[str] | None = None,
        requirements: list[str] | None = None,
        instructions: str | None = None,
        task_id: str | None = None,
        assisted_by: list[dict[str, Any]] | None = None,
        dry_run: bool = False,
        branch: str | None = None,
        if_match: str | None = None,
    ) -> dict[str, Any]:
        """Validate client-proposed candidates and, unless a dry run, commit them.

        All or nothing: a commit whose candidates did not all validate writes
        nothing and is ``rejected_candidates``, with every verdict, so the
        client can drop or fix the rejected ones and commit again.
        """
        source_id = _parse_block_id(source)
        try:
            parsed = [Candidate.model_validate(c) for c in candidates]
        except Exception as exc:  # noqa: BLE001 - pydantic detail is the message
            raise LifecycleError(USAGE, f"invalid candidates: {exc}") from exc
        if not parsed:
            raise LifecycleError(USAGE, "no candidates to commit")
        produced_by = None
        if producer is not None:
            try:
                produced_by = Producer.model_validate(producer)
            except Exception as exc:  # noqa: BLE001
                raise LifecycleError(USAGE, f"invalid producer: {exc}") from exc
            if produced_by.kind is ProducerKind.MODEL and not is_actor_id(
                produced_by.id
            ):
                raise LifecycleError(
                    USAGE,
                    f"producer id {produced_by.id!r} must name the model as "
                    f"'<namespace>/<model>', e.g. openai/gpt-4.1",
                )
        collaborators: list[Collaborator] = []
        for raw in assisted_by or []:
            try:
                collaborator = Collaborator.model_validate(raw)
            except Exception as exc:  # noqa: BLE001
                raise LifecycleError(USAGE, f"invalid assisting party: {exc}") from exc
            if not is_actor_id(collaborator.id) or (
                collaborator.model is not None and not is_actor_id(collaborator.model)
            ):
                raise LifecycleError(
                    USAGE,
                    f"assisting party {collaborator.id!r} (model {collaborator.model!r}) "
                    f"is not an actor identifier",
                )
            collaborators.append(collaborator)
        allowed_types = None
        if allowed is not None:
            try:
                allowed_types = [MemoryType(v) for v in allowed]
            except ValueError as exc:
                raise LifecycleError(USAGE, f"unknown memory type: {exc}") from exc

        def op(brain: Brain) -> dict[str, Any]:
            task = brain.define_task(
                source_id,
                allowed=allowed_types,
                requirements=requirements,
                instructions=instructions,
                task_id=task_id,
            )
            report = brain.validate(
                CandidateSet(task_id=task_id, producer=produced_by, candidates=parsed),
                task,
            )
            view = _report_view(report)
            if dry_run:
                return {
                    "dry_run": True,
                    "report": view,
                    "head": self._head_after(brain),
                }
            if not report.is_clean:
                raise LifecycleError(
                    REJECTED_CANDIDATES,
                    f"{len(report.results) - len(report.committable)} of "
                    f"{len(report.results)} candidates did not validate; nothing was "
                    f"committed",
                    detail={"report": view},
                )
            result = brain.commit(report)
            committed = []
            for item in report.committable:
                block = item.block
                committed.append(
                    {
                        "block_id": str(block.block_id),
                        "memory_type": block.MEMORY_TYPE.value,
                        "payload": block.payload(),
                    }
                )
            return {
                "dry_run": False,
                "report": view,
                "committed": committed,
                "provenance": [str(b) for b in result.provenance],
                "snapshot": str(result.snapshot.digest),
                "roots": {mt.value: str(r) for mt, r in result.roots.items()},
                "head": self._head_after(brain),
                "branch": branch or DEFAULT_BRANCH,
            }

        return await self._write(
            handle,
            "validating" if dry_run else "committing",
            op,
            actor=actor,
            branch=branch,
            if_match=if_match,
            assisted_by=collaborators,
        )

    # -- branches ---------------------------------------------------------------- #

    async def list_branches(
        self, handle: BrainHandle, *, remote: bool = False
    ) -> dict[str, Any]:
        heads = handle.heads()
        local = [
            {
                "name": b.name,
                "tag": b.tag,
                "snapshot": str(b.snapshot),
                "current": b.current,
                "published": _digest(b.published),
                "unpublished": b.published != b.snapshot,
            }
            for b in sorted(
                heads.branches.values(),
                key=lambda b: (b.name != DEFAULT_BRANCH, b.name),
            )
        ]
        out: dict[str, Any] = {"branches": local}
        if remote:
            try:
                tags = await self._client_factory().list_tags(handle.reference)
            except ReferenceNotFoundError:
                tags = []
            published = {}
            for tag in tags:
                name = branch_for_tag(tag, handle.meta.default_tag)
                if name is not None:
                    published[name] = tag
            out["remote"] = [
                {"name": name, "tag": tag, "local": name in heads.branches}
                for name, tag in sorted(published.items())
            ]
        return out

    async def create_branch(
        self,
        handle: BrainHandle,
        name: str,
        *,
        actor: Actor,
        start: str | None = None,
    ) -> dict[str, Any]:
        if (
            name != DEFAULT_BRANCH
            and tag_for_branch(handle.meta, name) == handle.meta.default_tag
        ):
            raise LifecycleError(
                USAGE,
                f"branch {name!r} would publish to {handle.meta.default_tag!r}, the tag "
                f"main publishes to",
            )

        def op(brain: Brain) -> dict[str, Any]:
            if brain.state() == {}:
                raise LifecycleError(
                    REFUSED_PROTOCOL,
                    "the brain has no history yet; commit something before branching",
                )
            info = brain.create_branch(name, at=start)
            return {
                "name": info.name,
                "tag": tag_for_branch(handle.meta, info.name),
                "snapshot": str(info.snapshot),
            }

        return await self._write(
            handle, f"creating branch {name}", op, actor=actor, switch=False
        )

    async def delete_branch(
        self, handle: BrainHandle, name: str, *, actor: Actor, force: bool = False
    ) -> dict[str, Any]:
        if name == DEFAULT_BRANCH:
            raise LifecycleError(USAGE, "main cannot be deleted")

        def op(brain: Brain) -> dict[str, Any]:
            heads = {info.name: info.snapshot for info in brain.branches()}
            if name not in heads:
                raise LifecycleError(NOT_FOUND, f"no branch named {name!r}")
            if brain.current_branch() == name:
                # The working copy's current branch is the service's business,
                # not the caller's: step off it rather than refuse.
                brain.checkout(DEFAULT_BRANCH)
            brain.delete_branch(name, force=force)
            return {
                "name": name,
                "snapshot": str(heads[name]),
                "note": "the registry tag, if one was pushed, is left in place",
            }

        return await self._write(
            handle, f"deleting branch {name}", op, actor=actor, switch=False
        )

    # -- distribution (jobs) ---------------------------------------------------- #

    def _vector_layers_loadable(self, manifest: Any) -> bool:
        return BrainCache._loads_vector_indices(manifest, None)

    def push(
        self, handle: BrainHandle, branch: str, *, actor: Actor, force: bool = False
    ) -> Job:
        self._require_source(handle.reference)

        async def body(ctx: JobContext) -> dict[str, Any]:
            tag = tag_for_branch(handle.meta, branch)
            ctx.stage("waiting for the brain")
            async with handle.writing(f"pushing {branch}"):
                brain = handle.writer()
                await asyncio.to_thread(self._switch_to, handle, brain, branch)
                if brain.state() == {}:
                    raise LifecycleError(
                        REFUSED_PROTOCOL, "the brain has no history to push"
                    )
                ctx.stage(f"pushing to {handle.reference}:{tag}")
                client = self._client_factory()
                try:
                    digest = await brain.push(
                        client, handle.reference, tag, force=force
                    )
                except Exception:
                    handle.reset_writer()
                    raise
                handle.align_default_branch(brain)
                snapshot = brain.snapshot().digest
            result: dict[str, Any] = {
                "reference": handle.reference,
                "branch": branch,
                "tag": tag,
                "digest": str(digest),
                "snapshot": str(snapshot),
                "forced": force,
                "indexed": False,
            }
            if self.cache is not None:
                self.cache.invalidate(handle.reference, tag)
            if branch == DEFAULT_BRANCH:
                ctx.stage("indexing")
                try:
                    entry = await self._catalog_factory(self.settings).publish(
                        f"{handle.reference}:{tag}",
                        description=handle.meta.description or handle.meta.title,
                        author=actor.id,
                        published_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    )
                    result["indexed"] = True
                    result["index_entry"] = entry.get("entry_digest")
                except Exception as exc:  # noqa: BLE001 - the push itself landed
                    logger.warning(
                        "Pushed {}:{} but could not index it: {}",
                        handle.reference,
                        tag,
                        exc,
                    )
                    result["index_error"] = str(exc)
            return result

        return self.jobs.submit(
            "push",
            brain=handle.reference,
            actor=actor.id,
            body=body,
            params={"branch": branch, "force": force},
        )

    def pull(
        self,
        handle: BrainHandle,
        *,
        actor: Actor,
        tag: str | None = None,
        branch: str | None = None,
    ) -> Job:
        """Install a published tag: main's by default, or the one a branch publishes to."""
        self._require_source(handle.reference)
        if tag is None:
            tag = tag_for_branch(handle.meta, branch or DEFAULT_BRANCH)

        async def body(ctx: JobContext) -> dict[str, Any]:
            ctx.stage("waiting for the brain")
            async with handle.writing(f"pulling {tag}"):
                result = await self._pull_into(handle, ctx, handle.reference, tag)
            if self.cache is not None:
                self.cache.invalidate(handle.reference, tag)
            return result

        return self.jobs.submit(
            "pull",
            brain=handle.reference,
            actor=actor.id,
            body=body,
            params={"tag": tag, "branch": branch},
        )

    async def _pull_into(
        self, handle: BrainHandle, ctx: JobContext, reference: str, tag: str
    ) -> dict[str, Any]:
        """Fetch (cancellable), then pull what was fetched (not cancellable)."""
        brain = handle.writer()
        if brain.state() and brain.reconcile_status() is not None:
            raise LifecycleError(
                NEEDS_HUMAN,
                "a convergence is open; pulling would move the head out from under it",
            )
        client = self._client_factory(pinned=True)
        ctx.stage(f"fetching {reference}:{tag}")
        manifest = await client.resolve(reference, tag)
        fetched = await brain.fetch(client, reference, tag)
        ctx.commit_point("installing")
        try:
            snapshot = await brain.pull(
                client,
                reference,
                tag,
                ignore_vector_indices=not self._vector_layers_loadable(manifest),
                verification=default_verification(self.settings),
            )
        except Exception:
            handle.reset_writer()
            raise
        handle.align_default_branch(brain)
        handle.forget_readers()
        return {
            "reference": reference,
            "tag": tag,
            "snapshot": str(snapshot.digest),
            "fetched": str(fetched.digest),
            "branch": brain.current_branch(),
            "governed": brain.trust_root is not None,
        }

    # -- converge ---------------------------------------------------------------- #

    def converge(
        self,
        handle: BrainHandle,
        *,
        actor: Actor,
        source: str,
        into: str = DEFAULT_BRANCH,
        strategy: str | None = None,
        reason: str | None = None,
        fast_forward: str = "auto",
    ) -> Job:
        """Bring *source* into branch *into*.

        *source* is a local branch name, a tag of this brain's repository, or
        a full ``registry/repo:tag`` reference. A remote history is fetched
        first (cancellable), then joined like a local branch: nothing when
        already contained, a fast-forward when *into* is behind, otherwise a
        reconciliation recorded with *strategy* -- required, never defaulted,
        because it decides who stays on record as author. A reconciliation
        that does not apply cleanly stops with the convergence open
        (``needs_human``); resolve it through ``/convergence``.
        """
        chosen = None
        if strategy is not None:
            try:
                chosen = ReconcileStrategy(strategy)
            except ValueError as exc:
                raise LifecycleError(
                    USAGE,
                    f"strategy must be one of {[s.value for s in ReconcileStrategy]}",
                ) from exc
        if fast_forward not in ("auto", "only", "never"):
            raise LifecycleError(USAGE, "fast_forward must be auto, only or never")

        async def body(ctx: JobContext) -> dict[str, Any]:
            ctx.stage("waiting for the brain")
            async with handle.writing(f"converging {source} into {into}"):
                brain = handle.writer()
                self._require_writable(brain)
                await asyncio.to_thread(self._switch_to, handle, brain, into)
                local = {info.name for info in brain.branches()}
                temp: str | None = None
                if source in local:
                    branch_name = source
                else:
                    reference, tag = self._remote(handle, source)
                    client = self._client_factory(pinned=True)
                    ctx.stage(f"fetching {reference}:{tag}")
                    fetched = await brain.fetch(client, reference, tag)
                    temp = f"converge/{ctx.job.id[-12:]}"
                    branch_name = temp
                    await asyncio.to_thread(brain.create_branch, temp, fetched.digest)
                ctx.commit_point("joining")

                def join() -> dict[str, Any]:
                    brain.actor = actor
                    try:
                        joined = brain.join(
                            branch_name,
                            chosen,
                            reason or f"converge {source} into {into}",
                            fast_forward=fast_forward,  # type: ignore[arg-type]
                        )
                    except ReconciliationHaltedError as exc:
                        status = brain.reconcile_status()
                        raise LifecycleError(
                            NEEDS_HUMAN,
                            f"converging {source} into {into} stopped with questions open; "
                            f"resolve them via /convergence, then continue or abort",
                            detail={"convergence": _dump(status)},
                        ) from exc
                    except BranchError as exc:
                        if chosen is None and "diverged" in str(exc):
                            raise LifecycleError(
                                USAGE,
                                f"{into!r} and {source!r} diverged; converging them needs "
                                f"a strategy (merge keeps their snapshots and signatures, "
                                f"rebase and squash re-sign the work as yours)",
                                detail={"needs": "strategy"},
                            ) from exc
                        raise
                    finally:
                        brain.actor = reader_actor()
                        if temp is not None:
                            try:
                                brain.delete_branch(temp, force=True)
                            except Exception as cleanup:  # noqa: BLE001
                                logger.warning("could not drop {}: {}", temp, cleanup)
                        handle.align_default_branch(brain)
                    out: dict[str, Any] = {
                        "source": source,
                        "into": into,
                        "outcome": joined.outcome,
                        "snapshot": str(joined.snapshot),
                        "strategy": chosen.value if chosen else None,
                    }
                    if joined.reconciliation is not None:
                        rec = joined.reconciliation
                        out["reconciliation"] = {
                            "parents": [str(p) for p in rec.parents],
                            "snapshots": [str(s) for s in rec.snapshots],
                            "admitted": {
                                mt.value: [str(b) for b in ids]
                                for mt, ids in rec.admitted.items()
                            },
                            "excluded": {
                                mt.value: [str(b) for b in ids]
                                for mt, ids in rec.excluded.items()
                            },
                        }
                    return out

                try:
                    return await asyncio.to_thread(join)
                except LifecycleError:
                    raise
                except Exception:
                    handle.reset_writer()
                    raise

        return self.jobs.submit(
            "converge",
            brain=handle.reference,
            actor=actor.id,
            body=body,
            params={
                "from": source,
                "into": into,
                "strategy": strategy,
                "fast_forward": fast_forward,
            },
        )

    def _remote(self, handle: BrainHandle, source: str) -> tuple[str, str]:
        """``(reference, tag)`` for a converge source that is not a local branch."""
        if "/" in source:
            from gaussia_brains_mcp_server.cache import split_reference

            expanded = self.settings.expand_brain_ref(source)
            reference, tag = split_reference(expanded)
            self._require_source(reference)
            return reference, tag
        return handle.reference, source

    # -- convergence in progress -------------------------------------------------- #

    def convergence(self, handle: BrainHandle) -> dict[str, Any]:
        heads = handle.heads()
        if heads.head is None:
            return {"open": False}
        with handle.reading(heads.head) as reader:
            status = reader.brain.reconcile_status()
        if status is None:
            return {"open": False}
        return {
            "open": True,
            "branch": heads.current,
            "resolved": status.is_resolved,
            "unresolved": [str(b) for b in status.unresolved],
            "removals_accepted": status.removals_accepted,
            "status": _dump(status),
        }

    async def _convergence_write(
        self,
        handle: BrainHandle,
        what: str,
        fn: Callable[[Brain], Any],
        *,
        actor: Actor,
    ) -> Any:
        def op(brain: Brain) -> Any:
            if brain.state() == {} or brain.reconcile_status() is None:
                raise LifecycleError(NOT_FOUND, "no convergence is open on this brain")
            return fn(brain)

        result = await self._write(handle, what, op, actor=actor, switch=False)
        handle.forget_readers()
        return result

    async def resolve(
        self,
        handle: BrainHandle,
        *,
        actor: Actor,
        block: str,
        kind: str,
        prefer: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        bid = _parse_block_id(block)
        try:
            resolution = ResolutionKind(kind)
        except ValueError as exc:
            raise LifecycleError(
                USAGE, f"kind must be one of {[k.value for k in ResolutionKind]}"
            ) from exc
        preferred = _parse_block_id(prefer) if prefer else None

        def fn(brain: Brain) -> dict[str, Any]:
            status = brain.reconcile_resolve(
                bid, resolution, prefer=preferred, reason=reason, actor=actor
            )
            return {
                "resolved": status.is_resolved,
                "unresolved": [str(b) for b in status.unresolved],
                "status": _dump(status),
            }

        return await self._convergence_write(
            handle, f"resolving {bid.short}", fn, actor=actor
        )

    async def accept_removals(
        self, handle: BrainHandle, *, actor: Actor, reason: str | None = None
    ) -> dict[str, Any]:
        def fn(brain: Brain) -> dict[str, Any]:
            status = brain.reconcile_accept_removals(reason=reason, actor=actor)
            return {
                "resolved": status.is_resolved,
                "removals_accepted": status.removals_accepted,
                "status": _dump(status),
            }

        return await self._convergence_write(
            handle, "accepting removals", fn, actor=actor
        )

    async def continue_convergence(
        self, handle: BrainHandle, *, actor: Actor
    ) -> dict[str, Any]:
        def fn(brain: Brain) -> dict[str, Any]:
            result = brain.reconcile_continue()
            return {
                "outcome": "reconciled",
                "snapshot": str(result.snapshot.digest),
                "strategy": result.strategy.value,
                "parents": [str(p) for p in result.parents],
                "admitted": {
                    mt.value: [str(b) for b in ids]
                    for mt, ids in result.admitted.items()
                },
                "excluded": {
                    mt.value: [str(b) for b in ids]
                    for mt, ids in result.excluded.items()
                },
            }

        return await self._convergence_write(
            handle, "continuing convergence", fn, actor=actor
        )

    async def abort_convergence(
        self, handle: BrainHandle, *, actor: Actor
    ) -> dict[str, Any]:
        def fn(brain: Brain) -> dict[str, Any]:
            status = brain.reconcile_status()
            brain.reconcile_abort()
            return {
                "aborted": True,
                "theirs": str(status.state.theirs) if status else None,
                "decisions": len(status.state.resolutions) if status else 0,
            }

        return await self._convergence_write(
            handle, "aborting convergence", fn, actor=actor
        )

    # -- import --------------------------------------------------------------- #

    def import_from_registry(
        self,
        brain_ref: str,
        *,
        actor: Actor,
        tag: str | None = None,
        source: str | None = None,
        default_tag: str | None = None,
        title: str = "",
        description: str = "",
    ) -> Job:
        """Migrate a published brain into the working store.

        *source* (``registry/repo:tag``) defaults to the brain's own reference
        at *tag* (or its default tag). A governed brain is imported as it is
        and served read-only (decision D10).
        """
        reference, ref_tag = self.repository(brain_ref)
        self._require_source(reference)
        if source is not None:
            from gaussia_brains_mcp_server.cache import split_reference

            src_ref, src_tag = split_reference(self.settings.expand_brain_ref(source))
            self._require_source(src_ref)
        else:
            src_ref, src_tag = reference, tag or ref_tag or default_tag or "latest"
        handle = self.store.create(
            BrainMeta(
                reference=reference,
                default_tag=validate_default_tag(default_tag or ref_tag or src_tag),
                title=title,
                description=description,
                created_by=actor.id,
                imported_from=f"{src_ref}:{src_tag}",
            )
        )

        async def body(ctx: JobContext) -> dict[str, Any]:
            async with handle.writing("importing"):
                result = await self._pull_into(handle, ctx, src_ref, src_tag)
            result["imported_from"] = f"{src_ref}:{src_tag}"
            return result

        return self.jobs.submit(
            "import",
            brain=reference,
            actor=actor.id,
            body=body,
            params={"source": f"{src_ref}:{src_tag}"},
            cleanup=self._discard_if_empty(reference, handle),
        )

    def _discard_if_empty(
        self, reference: str, handle: BrainHandle
    ) -> Callable[[], None]:
        """A failed or cancelled import leaves nothing behind."""

        def cleanup() -> None:
            if handle.heads().head is None:
                self.store.discard(reference)

        return cleanup

    def import_layout(
        self,
        brain_ref: str,
        archive: Path,
        *,
        actor: Actor,
        default_tag: str | None = None,
        title: str = "",
        description: str = "",
    ) -> Job:
        """Migrate a brain from an uploaded OCI layout (a ``.tar`` or ``.tar.gz``).

        The archive is a brain's layout directory as pyboltzmann writes it
        (``oci-layout``, ``index.json``, ``blobs/`` and its pointers), e.g. a
        CLI brain under ``~/.alquimia/registry/brains/<id>``. It is unpacked
        into a scratch directory, opened and verified before it replaces the
        new brain's empty layout.
        """
        reference, ref_tag = self.repository(brain_ref)
        self._require_source(reference)
        handle = self.store.create(
            BrainMeta(
                reference=reference,
                default_tag=validate_default_tag(default_tag or ref_tag or "latest"),
                title=title,
                description=description,
                created_by=actor.id,
                imported_from="upload",
            )
        )

        async def body(ctx: JobContext) -> dict[str, Any]:
            async with handle.writing("importing"):
                ctx.stage("unpacking")
                scratch = Path(
                    tempfile.mkdtemp(prefix=".import-", dir=handle.directory)
                )
                try:
                    root = await asyncio.to_thread(_unpack_layout, archive, scratch)
                    ctx.stage("verifying")
                    summary = await asyncio.to_thread(_verify_layout, root)
                    ctx.commit_point("installing")
                    await asyncio.to_thread(_replace_layout, handle.layout, root)
                finally:
                    shutil.rmtree(scratch, ignore_errors=True)
                    archive.unlink(missing_ok=True)
                handle.reset_writer()
                handle.forget_readers()
                handle.align_default_branch(handle.writer())
            return {"reference": reference, **summary}

        discard = self._discard_if_empty(reference, handle)

        def cleanup() -> None:
            archive.unlink(missing_ok=True)
            discard()

        return self.jobs.submit(
            "import",
            brain=reference,
            actor=actor.id,
            body=body,
            params={"source": "upload"},
            cleanup=cleanup,
        )


async def search_registry(
    settings: Settings, cache: BrainCache, brain_ref: str, query: str, **params: Any
) -> dict[str, Any]:
    """Search a brain not held here, through the read cache MCP uses."""
    expanded = settings.expand_brain_ref(brain_ref)
    brain = await cache.get_or_open(expanded)
    try:
        found = await asyncio.to_thread(search_brain, brain, query, **params)
    except SearchInputError as exc:
        raise LifecycleError(USAGE, str(exc)) from exc
    found["reference"] = expanded
    return found


# --------------------------------------------------------------------------- #
# Layout import helpers
# --------------------------------------------------------------------------- #


def _unpack_layout(archive: Path, scratch: Path) -> Path:
    """Unpack *archive* safely and return the directory holding ``oci-layout``."""
    try:
        tar = tarfile.open(archive, mode="r:*")
    except tarfile.TarError as exc:
        raise LifecycleError(USAGE, f"the upload is not a tar archive: {exc}") from exc
    with tar:
        for member in tar.getmembers():
            if not (member.isfile() or member.isdir()):
                raise LifecycleError(
                    USAGE,
                    f"the archive holds {member.name!r}, which is not a file or a directory",
                )
        try:
            tar.extractall(scratch, filter="data")
        except (tarfile.TarError, OSError) as exc:
            raise LifecycleError(
                USAGE, f"the archive could not be unpacked: {exc}"
            ) from exc
    markers = sorted(scratch.rglob("oci-layout"), key=lambda p: len(p.parts))
    if not markers:
        raise LifecycleError(
            USAGE, "the archive holds no OCI layout (no oci-layout file)"
        )
    return markers[0].parent


def _verify_layout(root: Path) -> dict[str, Any]:
    store = OciLayoutStore(root, create=False)
    brain = Brain(store, actor=reader_actor())
    if not brain.state():
        raise LifecycleError(USAGE, "the uploaded layout holds no brain history")
    if not brain.verify():
        raise LifecycleError(
            REFUSED_PROTOCOL, "the uploaded brain does not verify against its own roots"
        )
    snapshot = brain.snapshot()
    return {
        "snapshot": str(snapshot.digest),
        "governed": brain.trust_root is not None,
        "branches": [info.name for info in brain.branches()],
        "block_count": snapshot.block_count,
    }


def _replace_layout(target: Path, root: Path) -> None:
    backup = target.with_name(target.name + ".old")
    if target.exists():
        target.rename(backup)
    shutil.move(str(root), str(target))
    shutil.rmtree(backup, ignore_errors=True)
