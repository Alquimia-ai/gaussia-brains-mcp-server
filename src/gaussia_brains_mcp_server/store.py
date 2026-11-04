"""Constructing read-only brain handles.

Brains are always held in a ``MemoryBlockStore``: the registry is the durable
copy and this process is a cache, discarded with the pod.

Search quality is decided by the planner and indices. pyboltzmann ships none
-- ``planner=None`` gives its built-in verifying scan -- and the engines in
``alquimia.boltzmann.indices`` depend on ``alquimia.core`` (embeddings, text
folding). They are to be ported here behind :class:`IndexProvider`, with
query embeddings coming from a configurable embedding endpoint rather than
the Alquimia embeddings config (docs/proposal.md §7). Until then the scan
answers every query, correctly but without semantic ranking.
"""

from __future__ import annotations

import threading
from typing import Any, Protocol

from boltzmann import Actor, ActorKind, Brain, MemoryBlockStore, MemoryType
from boltzmann.indices import Index

from gaussia_brains_mcp_server.config import get_settings


class IndexProvider(Protocol):
    """Supplies the planner and indices a brain is opened with."""

    def indices(self) -> dict[MemoryType, list[Index]] | None: ...

    def planner(self, indices: dict[MemoryType, list[Index]] | None) -> Any | None: ...

    def accepts_vector_index(self, embedding_model: str | None) -> bool:
        """Whether a travelling vector index embedded by *embedding_model* can be
        queried here. ``False`` makes the pull skip vector layers (the SDK
        fails a whole pull on a layer it cannot load)."""
        ...


class ScanIndexProvider:
    """No indices, pyboltzmann's built-in scan planner."""

    def indices(self) -> dict[MemoryType, list[Index]] | None:
        return None

    def planner(self, indices: dict[MemoryType, list[Index]] | None) -> Any | None:
        return None

    def accepts_vector_index(self, embedding_model: str | None) -> bool:
        return False


_provider: IndexProvider = ScanIndexProvider()
_provider_lock = threading.Lock()


def set_index_provider(provider: IndexProvider | None) -> None:
    """Install the provider every brain is opened with (``None`` restores the scan)."""
    global _provider
    with _provider_lock:
        _provider = provider or ScanIndexProvider()


def get_index_provider() -> IndexProvider:
    with _provider_lock:
        return _provider


def reader_actor() -> Actor:
    """The actor a read-only handle is opened as; it attributes nothing."""
    return Actor(
        id=get_settings().reader_actor_id,
        kind=ActorKind.SERVICE,
        name="read-only session",
    )


def build_brain(actor: Actor | None = None) -> Brain:
    """A fresh, empty, in-memory brain ready to be pulled into."""
    provider = get_index_provider()
    indices = provider.indices()
    return Brain(
        MemoryBlockStore(),
        actor=actor or reader_actor(),
        planner=provider.planner(indices),
        indices=indices,
    )
