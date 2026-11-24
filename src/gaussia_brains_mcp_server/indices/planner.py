"""Hybrid query planner for the Boltzmann Brain protocol (ported from core).

Divergences from core, each deliberate:

* ``truncated`` and ``verified_against`` are populated on the bundle (core
  left both at their defaults, so a client could never tell a cut answer).
* Range routing only fires on a query that parses as a range (``a..b`` or
  ``>x`` as a single token) and the parsed bounds are handed to the B+ tree;
  core routed any text containing ``..`` or starting with ``<``/``>`` to the
  structural indices alone, so prose such as "deploy failed... why?" found
  nothing. In AUTO mode a query the structural indices cannot answer falls
  back to the inverted index.
* The bundle carries a :class:`PlanReport` saying which index kinds were
  consulted and whether vector search took part, so ``narrowing.semantic``
  reports what happened rather than what was possible.

The planner implements the ``QueryPlanner`` protocol.  It reads the **same
index objects** the brain rebuilds, never its own copies.  Index selection is
driven by query shape: exact identifier → hash map; ordered / range predicate
→ B⁺ tree; term match → inverted; natural-language intent → vector;
neighbourhood or traversal → graph. (Bitmap, for categorical filters, is
never routed to here or in core, and is not registered.)

Every result path ends in hash-map resolution and verification, per the
conformance invariants.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from boltzmann import BlockId, IndexKind, MemoryType
from boltzmann.exceptions import (
    BlockIntegrityError,
    BlockNotFoundError,
    BlockTombstonedError,
    MembershipError,
    MemoryTypeError,
)
from boltzmann.indices import Index
from boltzmann.query import (
    EvidenceBundle,
    Match,
    Query,
    QueryHints,
    QueryPlanner,
    RetrievalMode,
    SourceRef,
)
from loguru import logger
from pydantic import PrivateAttr

from gaussia_brains_mcp_server.indices.engines import _score_to_str as _score_str

_RANGE_PAIR = re.compile(r"^([^\s.]+)\.\.([^\s.]+)$")
_RANGE_BOUND = re.compile(r"^(>=|<=|>|<)([^\s<>=]+)$")


@dataclass
class PlanReport:
    """What the planner did for one query (not part of the protocol bundle).

    Attributes:
        routed: Index kinds the query shape selected, in priority order.
        consulted: Index kinds that were actually searched (an index the
            module does not carry is skipped), including any fallback.
        contributed: Index kinds that returned at least one candidate.
        fallback: The index kind added because the routed ones found nothing.
        vector_used: Whether a vector index scored the query for some module.
        vector_reason: Why vector search did not take part, when it did not:
            ``not_routed``, ``no_vector_index``, ``no_vectors``,
            ``model_mismatch``, ``embedding_failed`` or ``not_text``.
    """

    routed: list[str] = field(default_factory=list)
    consulted: list[str] = field(default_factory=list)
    contributed: list[str] = field(default_factory=list)
    fallback: str | None = None
    vector_used: bool = False
    vector_reason: str | None = "not_routed"


class PlannedEvidenceBundle(EvidenceBundle):
    """An ``EvidenceBundle`` that also carries the :class:`PlanReport`.

    A private attribute, so the bundle serializes exactly like the SDK's and
    survives ``Brain.search``'s ``model_copy`` (private state is copied).
    """

    _plan: PlanReport | None = PrivateAttr(default=None)

    @property
    def plan(self) -> PlanReport | None:
        return self._plan


def plan_report(bundle: EvidenceBundle) -> PlanReport | None:
    """The planner's report for *bundle*, or ``None`` from another planner."""
    return (
        getattr(bundle, "_plan", None)
        if isinstance(bundle, PlannedEvidenceBundle)
        else None
    )


class HybridQueryPlanner(QueryPlanner):
    """Select indices and fuse rankings according to query shape.

    Args:
        indices: ``dict[MemoryType, list[Index]]`` — the same map passed to
            ``Brain(indices=...)``.  The planner reads from this dict in-place;
            it never copies or clones the index objects.
    """

    def __init__(self, indices: dict[MemoryType, list[Index]]) -> None:
        self._indices = indices

    # ------------------------------------------------------------------ #
    # Index helpers
    # ------------------------------------------------------------------ #

    def _pick(
        self,
        memory_type: MemoryType,
        kind: IndexKind,
    ) -> Index | None:
        for idx in self._indices.get(memory_type, []):
            if idx.kind is kind:
                return idx
        return None

    # ------------------------------------------------------------------ #
    # Query-shape analysis
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_block_id(text: str) -> bool:
        """Heuristic: a 64-hex-char string is likely a block id."""
        return bool(re.fullmatch(r"[0-9a-f]{64}", text.lower()))

    @staticmethod
    def _as_block_id(text: str) -> BlockId | None:
        """Parse a block id, accepting bare hex as well as ``sha256:<hex>``."""
        for candidate in (text, f"sha256:{text}"):
            try:
                return BlockId.parse(candidate)
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_range(text: str) -> dict[str, str] | None:
        """B+ tree bounds for a range-shaped query, or ``None``.

        Only a single token counts: ``a..b`` (inclusive) or ``>x`` / ``>=x`` /
        ``<x`` / ``<=x``. Diverges from core, which treated any text containing
        ``..`` or starting with ``<``/``>`` as a range and searched it
        structurally only -- so prose with an ellipsis matched nothing.
        """
        text = text.strip()
        if m := _RANGE_PAIR.match(text):
            return {"gte": m.group(1), "lte": m.group(2)}
        if m := _RANGE_BOUND.match(text):
            op = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}[m.group(1)]
            return {op: m.group(2)}
        return None

    @classmethod
    def _looks_like_range(cls, text: str) -> bool:
        return cls._parse_range(text) is not None

    @staticmethod
    def _looks_like_nl(text: str) -> bool:
        """Heuristic: >3 words and at least one space → natural language."""
        words = text.strip().split()
        return len(words) > 3

    @staticmethod
    def _looks_like_term_query(text: str) -> bool:
        """Short, keyword-like query."""
        words = text.strip().split()
        return 0 < len(words) <= 3

    @staticmethod
    def _wants_graph_expansion(query: Query) -> bool:
        """Whether the caller asked for neighbourhood expansion."""
        hints = query.hints or QueryHints()
        return hints.expand_depth > 0

    # ------------------------------------------------------------------ #
    # Routing
    # ------------------------------------------------------------------ #

    def _route_text(self, text: str, mode: RetrievalMode) -> list[IndexKind]:
        """Return a list of index kinds to consult, in priority order."""
        if mode is RetrievalMode.EXACT:
            return [IndexKind.HASH_MAP]
        if mode is RetrievalMode.LEXICAL:
            return [IndexKind.INVERTED, IndexKind.HASH_MAP]
        if mode is RetrievalMode.SEMANTIC:
            return [IndexKind.VECTOR, IndexKind.HASH_MAP]
        if mode is RetrievalMode.ASSOCIATIVE:
            return [IndexKind.GRAPH, IndexKind.HASH_MAP]
        # AUTO
        if self._is_block_id(text):
            return [IndexKind.HASH_MAP]
        if self._looks_like_range(text):
            return [IndexKind.BTREE, IndexKind.HASH_MAP]
        if self._looks_like_nl(text):
            return [IndexKind.VECTOR, IndexKind.INVERTED, IndexKind.HASH_MAP]
        if self._looks_like_term_query(text):
            return [IndexKind.INVERTED, IndexKind.HASH_MAP]
        return [IndexKind.INVERTED, IndexKind.VECTOR, IndexKind.HASH_MAP]

    # ------------------------------------------------------------------ #
    # Fusion
    # ------------------------------------------------------------------ #

    @staticmethod
    def _fuse(
        rankings: list[list[tuple[BlockId, float]]],
        limit: int,
    ) -> dict[BlockId, float]:
        """Reciprocal rank fusion (RRF) with k=60.

        Scores are already string-encoded at the index layer to satisfy the
        SDK's determinism invariant.
        """
        k = 60
        fused: dict[BlockId, float] = {}
        for ranking in rankings:
            for rank, (bid, _score) in enumerate(ranking, start=1):
                fused[bid] = fused.get(bid, 0.0) + 1.0 / (k + rank)
        # Re-apply the original best score as a tie-breaker so that two
        # blocks with the same RRF score are ordered by their highest
        # individual retrieval score, and then by block id, so the cut at
        # *limit* keeps the same blocks in every process.
        best_score: dict[BlockId, float] = {}
        for ranking in rankings:
            for bid, score in ranking:
                best_score[bid] = max(best_score.get(bid, 0.0), score)
        sorted_items = sorted(
            fused.items(),
            key=lambda item: (-item[1], -best_score.get(item[0], 0.0), str(item[0])),
        )
        return dict(sorted_items[:limit])

    # ------------------------------------------------------------------ #
    # Verification
    # ------------------------------------------------------------------ #

    @staticmethod
    def _sources(block: Any) -> list[SourceRef]:
        """The canonical evidence this block cites.

        ``evidence`` is a list of canonical block ids on semantic, episodic and
        procedural blocks. The locator (a page, a line range, a timestamp) lives
        on the provenance derivation record rather than on the block, so it is
        left unset here: the planner reads modules, not the ledger.
        """
        evidence = getattr(block, "evidence", None) or []
        return [SourceRef(block_id=bid) for bid in evidence]

    def _verify(
        self,
        memory_type: MemoryType,
        module: Any,
        candidates: dict[BlockId, float],
    ) -> list[Match]:
        """Resolve every candidate against the installed composition.

        Verification is done against the module, not against an index. An index
        is a derived view and the paper is explicit that no single index may be
        treated as authoritative -- an index that has drifted from the
        composition is precisely the case verification exists to catch, so
        asking it whether it is right answers nothing.

        ``Module.get`` checks membership before fetching bytes, and decoding
        re-derives the block id, so both halves of the invariant (hash *and*
        membership in the installed snapshot) are covered. ``verified`` is set
        from that result rather than asserted.

        A tombstoned block is reported with ``resolvable=False`` rather than
        dropped, so that a redacted block never looks like a corrupted one
        (paper Section 10.6).
        """
        matches: list[Match] = []
        for bid, score in candidates.items():
            score_str = _score_str(score)
            try:
                block = module.get(bid)
            except BlockTombstonedError:
                matches.append(
                    Match(
                        block_id=bid,
                        memory_type=memory_type,
                        content={},
                        score=score_str,
                        verified=False,
                        resolvable=False,
                    )
                )
                continue
            except (
                MembershipError,
                MemoryTypeError,
                BlockNotFoundError,
                BlockIntegrityError,
            ) as exc:
                # Not part of what this root commits to, or unreadable. The
                # index offered it, the composition did not: drop it and say so.
                logger.warning(
                    f"planner: dropping candidate {bid.short} from "
                    f"{memory_type.value}: {type(exc).__name__}: {exc}"
                )
                continue

            matches.append(
                Match(
                    block_id=bid,
                    memory_type=memory_type,
                    content=block.payload(),
                    score=score_str,
                    sources=self._sources(block),
                    verified=block.block_id == bid,
                    resolvable=module.store.is_resolvable(bid),
                )
            )
        return matches

    # ------------------------------------------------------------------ #
    # Protocol implementation
    # ------------------------------------------------------------------ #

    def plan(
        self,
        query: Query,
        modules: dict[MemoryType, Any],
    ) -> EvidenceBundle:
        """Plan a query over the installed modules.

        Args:
            query: The user's query.
            modules: ``dict[MemoryType, Module]`` — currently installed modules.

        Returns:
            EvidenceBundle: verified matches with Merkle roots recorded.
        """
        hints = query.hints or QueryHints()
        limit = hints.limit
        mode = hints.mode
        text = (query.text or "").strip()

        report = PlanReport()
        truncated = False
        matches: list[Match] = []

        # Determine which memory types to search
        memory_types = list(modules.keys())
        if query.filters and query.filters.memory_types:
            memory_types = [
                mt for mt in memory_types if mt in query.filters.memory_types
            ]

        # One ranking per index kind, across every module, then fusion. RRF
        # scores depend only on rank, so fusing per module and merging the
        # results would tie each module's best match regardless of relevance.
        kinds = self._route_text(text, mode)
        report.routed = [k.value for k in kinds]
        if IndexKind.VECTOR in kinds:
            report.vector_reason = "no_vector_index"
        owners: dict[BlockId, list[MemoryType]] = {}
        rankings: list[list[tuple[BlockId, float]]] = []
        range_query = self._parse_range(text)

        def consult(kind: IndexKind) -> None:
            nonlocal truncated
            merged: dict[BlockId, float] = {}
            for mt in memory_types:
                idx = self._pick(mt, kind)
                if idx is None:
                    logger.debug(f"planner: no {kind.value} index for {mt.value}")
                    continue
                if kind.value not in report.consulted:
                    report.consulted.append(kind.value)
                # One more than the limit, so a full page says whether more exist.
                probe = limit + 1
                if kind is IndexKind.BTREE and range_query is not None:
                    found = idx.search(range_query, limit=probe)
                elif kind is IndexKind.VECTOR and hasattr(idx, "search_with_status"):
                    found, reason = idx.search_with_status(text, limit=probe)
                    if reason is None:
                        report.vector_used = True
                        report.vector_reason = None
                    elif not report.vector_used:
                        report.vector_reason = reason
                else:
                    found = idx.search(text, limit=probe)
                for bid, score in found:
                    offered_by = owners.setdefault(bid, [])
                    if mt not in offered_by:
                        offered_by.append(mt)
                    merged[bid] = max(score, merged.get(bid, score))
            if merged:
                report.contributed.append(kind.value)
                ranked = sorted(merged.items(), key=lambda i: (-i[1], str(i[0])))
                if len(ranked) > limit:
                    truncated = True
                rankings.append(ranked[:limit])

        for kind in kinds:
            if kind is IndexKind.GRAPH:
                continue  # expanded after fusion, from the strongest hits
            consult(kind)

        # A query the structural indices could not answer (a range or an id
        # that matched nothing) is still text: try the terms. AUTO only -- an
        # explicit mode is the caller's choice of index. Diverges from core.
        if (
            not rankings
            and mode is RetrievalMode.AUTO
            and IndexKind.INVERTED not in kinds
            and text
        ):
            report.fallback = IndexKind.INVERTED.value
            consult(IndexKind.INVERTED)

        if len({bid for ranking in rankings for bid, _ in ranking}) > limit:
            truncated = True
        fused = self._fuse(rankings, limit) if rankings else {}
        by_module: dict[MemoryType, dict[BlockId, float]] = {}
        for bid, score in fused.items():
            # Verified against every module that offered it. A module only
            # holds blocks of its own memory type, so for an id offered by
            # several, every module but its own raises MemoryTypeError in
            # _verify and drops it: at most one Match per id.
            for mt in owners[bid]:
                by_module.setdefault(mt, {})[bid] = score

        for mt in memory_types:
            candidates = self._expand_graph(
                mt, by_module.get(mt, {}), hints, limit, text
            )
            if candidates:
                matches.extend(self._verify(mt, modules[mt], candidates))

        # `limit` is the caller's top-k over the query. Graph expansion runs
        # per module after fusion, so the union can exceed it; ties break on
        # the block id so equal scores do not reorder between processes.
        matches.sort(key=lambda m: (-float(m.score or 0.0), str(m.block_id)))
        if len(matches) > limit:
            truncated = True
            del matches[limit:]

        verified_against = {}
        for mt in memory_types:
            root = getattr(modules[mt], "root", None)
            if root is not None:
                verified_against[mt] = root

        bundle = PlannedEvidenceBundle(
            matches=matches,
            verified_against=verified_against,
            truncated=truncated,
        )
        bundle._plan = report
        return bundle

    def _expand_graph(
        self,
        memory_type: MemoryType,
        fused: dict[BlockId, float],
        hints: QueryHints,
        limit: int,
        text: str = "",
    ) -> dict[BlockId, float]:
        """Expand the neighbourhood of the strongest hits.

        The graph runs last because it traverses *from* block ids: it has
        nothing to work with until the other indices have said which blocks
        matter. When nothing matched but the query is itself a block id, that
        id is the seed -- which is what makes a pure associative query ("the
        neighbourhood of this block") answerable on its own.
        """
        depth = hints.expand_depth
        if depth <= 0:
            return fused

        idx = self._pick(memory_type, IndexKind.GRAPH)
        if idx is None:
            return fused

        if not fused:
            seed = self._as_block_id(text)
            if seed is None:
                return fused
            fused = {seed: 1.0}

        floor = min(fused.values())
        neighbours: dict[BlockId, float] = {}
        for seed in list(fused):
            for bid, score in idx.search({"seed": seed, "depth": depth}, limit=limit):
                if bid in fused:
                    continue
                neighbours[bid] = max(neighbours.get(bid, 0.0), score)

        if not neighbours:
            return fused

        # Keep expansion strictly below the matched set, preserving its order.
        expanded = dict(fused)
        for rank, (bid, _distance) in enumerate(
            sorted(neighbours.items(), key=lambda item: (-item[1], str(item[0]))),
            start=1,
        ):
            expanded[bid] = floor * (0.5**rank)
        return dict(list(expanded.items())[: max(limit, len(fused))])
