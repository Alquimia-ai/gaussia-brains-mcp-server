"""What ``search_brain`` reports about its own answer: truncation, routing, semantic."""

from __future__ import annotations

import pytest
from boltzmann import Actor, ActorKind, Brain, MemoryType
from boltzmann.blocks import Producer
from boltzmann.ingest import Candidate, CandidateSet, RegistrationRequest
from boltzmann.store import MemoryBlockStore

from gaussia_brains_mcp_server.indices import EngineIndexProvider
from gaussia_brains_mcp_server.indices.engines import ENV
from gaussia_brains_mcp_server.indices.planner import HybridQueryPlanner
from gaussia_brains_mcp_server.search import search_brain

ANA = Actor(id="ana@example.com", kind=ActorKind.HUMAN)


class ToggleEmbeddings:
    """Deterministic embeddings that can be made to fail after the build."""

    model_tag = "test/toggle"

    def __init__(self) -> None:
        self.fail = False

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if self.fail:
            raise RuntimeError("embeddings endpoint down")
        return [
            [
                1.0 if "deploy" in t.lower() else 0.0,
                1.0 if "cat" in t.lower() else 0.0,
                0.1,
            ]
            for t in texts
        ]


@pytest.fixture
def embeddings():
    previous = ENV.embeddings
    ENV.embeddings = ToggleEmbeddings()
    yield ENV.embeddings
    ENV.embeddings = previous


@pytest.fixture
def no_embeddings():
    previous = ENV.embeddings
    ENV.embeddings = None
    yield
    ENV.embeddings = previous


def _brain() -> Brain:
    provider = EngineIndexProvider()
    indices = provider.indices()
    return Brain(
        MemoryBlockStore(),
        actor=ANA,
        planner=provider.planner(indices),
        indices=indices,
    )


def _seed(brain: Brain, statements: list[str]) -> None:
    source = brain.register(
        b"facts", RegistrationRequest(media_type="text/plain", actor=ANA)
    )
    task = brain.define_task(source.block_id, allowed=[MemoryType.SEMANTIC])
    report = brain.validate(
        CandidateSet(
            producer=Producer(kind="model", id="openai/gpt-4.1"),
            candidates=[
                Candidate(
                    memory_type="semantic",
                    payload={"kind": "fact", "label": f"f{i}", "statement": text},
                    evidence=[source.block_id],
                )
                for i, text in enumerate(statements)
            ],
        ),
        task,
    )
    brain.commit(report)


def test_truncated_when_more_matches_than_limit(no_embeddings) -> None:
    brain = _brain()
    _seed(brain, [f"Guanciale fact number {i}." for i in range(40)])
    found = search_brain(brain, "guanciale", limit=10)
    assert len(found["results"]) == 10
    assert found["truncated"] is True


def test_not_truncated_when_everything_fits(no_embeddings) -> None:
    brain = _brain()
    _seed(brain, [f"Guanciale fact number {i}." for i in range(5)] + ["Sourdough."])
    found = search_brain(brain, "guanciale", limit=10)
    assert len(found["results"]) == 5
    assert found["truncated"] is False
    assert "semantic" in found["verified_against"]


def test_semantic_true_only_when_vectors_scored_the_query(embeddings) -> None:
    brain = _brain()
    _seed(brain, ["The deploy pipeline broke.", "The cat sleeps."])

    nl = search_brain(brain, "why did the deploy pipeline break")
    assert nl["narrowing"]["semantic"] is True
    assert nl["narrowing"]["semantic_reason"] is None
    assert "vector" in nl["narrowing"]["indices_used"]

    short = search_brain(brain, "deploy")
    assert short["narrowing"]["semantic"] is False
    assert short["narrowing"]["semantic_reason"] == "not_routed"

    lexical = search_brain(brain, "why did the deploy pipeline break", mode="lexical")
    assert lexical["narrowing"]["semantic"] is False
    assert lexical["narrowing"]["semantic_reason"] == "not_routed"

    embeddings.fail = True
    failed = search_brain(brain, "why did the deploy pipeline break")
    assert failed["narrowing"]["semantic"] is False
    assert failed["narrowing"]["semantic_reason"] == "embedding_failed"
    # The inverted index still answers.
    assert failed["results"]


def test_semantic_false_without_a_vector_index(no_embeddings) -> None:
    brain = _brain()
    _seed(brain, ["The deploy pipeline broke."])
    found = search_brain(brain, "why did the deploy pipeline break")
    assert found["narrowing"]["semantic"] is False
    assert found["narrowing"]["semantic_reason"] == "no_vector_index"
    assert found["narrowing"]["indices_used"] == ["inverted"]


def test_prose_with_an_ellipsis_is_not_a_range(no_embeddings) -> None:
    brain = _brain()
    _seed(brain, ["The deploy failed on Monday.", "Sourdough needs a starter."])
    found = search_brain(brain, "deploy failed... why?")
    assert found["results"]
    assert "deploy" in found["results"][0]["content"].lower()


def test_range_parsing() -> None:
    parse = HybridQueryPlanner._parse_range
    assert parse("2024-01-01..2024-12-31") == {"gte": "2024-01-01", "lte": "2024-12-31"}
    assert parse(">=5") == {"gte": "5"}
    assert parse("<b") == {"lt": "b"}
    assert parse("deploy failed... why?") is None
    assert parse("> not a range") is None
    assert parse("<html>") is None


def test_unmatched_range_falls_back_to_terms(no_embeddings) -> None:
    brain = _brain()
    _seed(brain, ["Release 2024..2025 notes.", "Sourdough needs a starter."])
    found = search_brain(brain, "2024..2025")
    assert found["results"]
    assert "inverted" in found["narrowing"]["indices_used"]
