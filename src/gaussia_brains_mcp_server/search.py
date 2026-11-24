"""One search function for MCP ``brain_search`` and REST ``POST .../search``.

Both surfaces run a query through this, over a brain opened with the same
index provider, so an agent and a human browsing in Ledger get the same top-k.
Besides the matches it reports what limited the answer:

* ``truncated`` -- the planner cut results at ``limit``;
* ``narrowing`` -- requested memory types the brain does not hold, and whether
  semantic (vector) search took part *in this query*. Without an embeddings
  endpoint, with vectors embedded by another model, when the query was routed
  past the vector index (a short keyword query, ``mode="lexical"``, memory
  types without a vector index) or when embedding the query failed, the answer
  came from the structural indices alone, and a client should be able to say
  so. ``semantic_reason`` says why; ``indices_used`` names the index kinds that
  returned candidates.
"""

from __future__ import annotations

from typing import Any, cast

from boltzmann import Brain, IndexKind, MemoryType
from boltzmann.query import Query, QueryFilters, QueryHints, RetrievalMode

from gaussia_brains_mcp_server.guidance import content_to_text
from gaussia_brains_mcp_server.indices.engines import _block_text
from gaussia_brains_mcp_server.indices.planner import plan_report

MAX_LIMIT = 100
MAX_EXPAND_DEPTH = 3


class SearchInputError(ValueError):
    """The query parameters are not acceptable (``QUERY_INVALID`` / ``usage``)."""


def _semantic_ready(brain: Brain) -> bool:
    """Whether a vector index over the semantic module *could* answer here.

    Only a fallback for a brain opened with a planner that does not report
    what it did; :func:`plan_report` is the answer for our own planner.
    """
    if not brain.snapshot().has_module(MemoryType.SEMANTIC):
        return False
    for index in brain.indices.get(MemoryType.SEMANTIC, []):
        if index.kind is IndexKind.VECTOR:
            matches = getattr(index, "_model_matches", None)
            vectors = getattr(index, "_vectors", None)
            return bool(vectors) and (matches() if callable(matches) else True)
    return False


def _match_text(brain: Brain, match: Any) -> str:
    """A match as text: a block that names its content (a canonical source)
    renders that content, not the descriptor that points at it."""
    content = match.content
    if not isinstance(content, dict):
        return str(content)
    if match.resolvable and ("blob" in content or "content" in content):
        try:
            block = brain.resolve(match.block_id)
            if block.content_digests:
                text = _block_text(block, brain.store)
                if text:
                    return text
        except Exception:  # noqa: BLE001 - fall back to the payload
            pass
    return content_to_text(content)


def search_brain(
    brain: Brain,
    query: str,
    *,
    memory_types: list[str] | None = None,
    limit: int = 10,
    expand_depth: int = 0,
    mode: str = "auto",
    include_unverified: bool = False,
) -> dict[str, Any]:
    """Run *query* over *brain* and return matches plus what narrowed them."""
    if not isinstance(query, str):
        raise SearchInputError("query must be a string")
    if not 1 <= limit <= MAX_LIMIT:
        raise SearchInputError(f"limit must be between 1 and {MAX_LIMIT}")
    if not 0 <= expand_depth <= MAX_EXPAND_DEPTH:
        raise SearchInputError(f"expand_depth must be between 0 and {MAX_EXPAND_DEPTH}")
    try:
        retrieval = RetrievalMode(mode)
    except ValueError as exc:
        raise SearchInputError(
            f"mode must be one of {[m.value for m in RetrievalMode]}"
        ) from exc

    installed = set(brain.snapshot().installed)
    filters: dict[str, Any] = {}
    requested: list[MemoryType] | None = None
    if memory_types is not None:
        try:
            requested = [MemoryType(v) for v in memory_types]
        except ValueError as exc:
            raise SearchInputError(f"invalid memory type: {exc}") from exc
        filters["memory_types"] = requested
    searched = [mt for mt in (requested or sorted(installed)) if mt in installed]
    missing = [mt for mt in (requested or []) if mt not in installed]

    bundle = brain.search(
        Query(
            text=query,
            filters=cast(QueryFilters, filters),
            hints=QueryHints(limit=limit, expand_depth=expand_depth, mode=retrieval),
        )
    )

    results: list[dict[str, Any]] = []
    dropped = 0
    for match in bundle.matches:
        if not match.verified and not include_unverified:
            dropped += 1
            continue
        sources = list(match.sources or [])
        results.append(
            {
                "block_id": str(match.block_id),
                "memory_type": match.memory_type.value,
                "content": _match_text(brain, match),
                "score": match.score,
                "sources": [
                    {"block_id": str(src.block_id), "locator": src.locator}
                    for src in sources
                ],
                "locator": next((src.locator for src in sources), None),
                "resolvable": match.resolvable,
                "verified": match.verified,
            }
        )

    report = plan_report(bundle)
    if report is not None:
        semantic = report.vector_used
        semantic_reason = report.vector_reason
        indices_used: list[str] | None = list(report.contributed)
    else:
        semantic = _semantic_ready(brain)
        semantic_reason = None if semantic else "unavailable"
        indices_used = None
    return {
        "results": results,
        "truncated": bool(bundle.truncated),
        "verified_against": {
            mt.value: str(root) for mt, root in bundle.verified_against.items()
        },
        "narrowing": {
            "requested_memory_types": [mt.value for mt in requested]
            if requested is not None
            else None,
            "searched_memory_types": [mt.value for mt in searched],
            "missing_memory_types": [mt.value for mt in missing],
            "semantic": semantic,
            "semantic_reason": semantic_reason,
            "indices_used": indices_used,
            "dropped_unverified": dropped,
        },
    }
