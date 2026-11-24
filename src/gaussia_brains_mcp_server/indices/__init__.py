"""Search engines shared by MCP (``brain_search``) and REST (``POST .../search``).

Six index kinds ported from ``alquimia.boltzmann.indices`` and the hybrid
planner that routes a query by shape and fuses rankings (RRF). Every brain this
service opens -- a registry brain in the read cache or a working-store brain --
is opened with :class:`EngineIndexProvider`, so both surfaces answer a query
with the same engine.
"""

from __future__ import annotations

from typing import Any

from boltzmann import MemoryType
from boltzmann.indices import Index

from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.embeddings import get_embeddings
from gaussia_brains_mcp_server.indices.engines import (
    ENV,
    BitmapIndex,
    BTreeIndex,
    GraphIndex,
    HashMapIndex,
    InvertedIndex,
    VectorIndex,
)
from gaussia_brains_mcp_server.indices.planner import HybridQueryPlanner

__all__ = [
    "BTreeIndex",
    "BitmapIndex",
    "EngineIndexProvider",
    "GraphIndex",
    "HashMapIndex",
    "HybridQueryPlanner",
    "InvertedIndex",
    "VectorIndex",
    "configure_engines",
]


def configure_engines(settings: Settings) -> None:
    """Point the engines at the configured ceilings and embeddings endpoint."""
    ENV.content_max_chars = settings.content_max_chars
    ENV.index_max_chars = settings.index_max_chars
    ENV.embeddings = get_embeddings(settings)


class EngineIndexProvider:
    """The index set and planner every brain is opened with.

    The same set core opens a brain with (hash map, B+ tree and inverted index
    on every module, the vector index on semantic where an embeddings model is
    configured) plus the graph index, so ``expand_depth`` follows relations.
    ``VectorIndex.build`` refuses to build without embeddings, so it is only
    registered where it can be built.

    ``BitmapIndex`` is ported but deliberately not registered, as in core
    (``alquimia.boltzmann.store.default_indices``): the planner never routes
    a query to ``IndexKind.BITMAP`` (``QueryFilters`` carries no categorical
    predicate to hand it), so building it would cost a pass over every block
    for an index nothing reads.
    """

    def indices(self) -> dict[MemoryType, list[Index]] | None:
        indices: dict[MemoryType, list[Index]] = {
            mt: [HashMapIndex(), BTreeIndex(), InvertedIndex(), GraphIndex()]
            for mt in MemoryType
        }
        if VectorIndex.is_available():
            indices[MemoryType.SEMANTIC].append(VectorIndex())
        return indices

    def planner(self, indices: dict[MemoryType, list[Index]] | None) -> Any | None:
        return HybridQueryPlanner(indices or {})

    def accepts_vector_index(self, embedding_model: str | None) -> bool:
        return VectorIndex.accepts(embedding_model)

    def semantic_available(self) -> bool:
        return VectorIndex.is_available()
