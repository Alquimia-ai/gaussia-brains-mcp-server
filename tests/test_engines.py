"""The ported search engines: tokenizer, inverted ranking, vector index, embeddings."""

from __future__ import annotations

import json

import httpx
import pytest
from boltzmann import Actor, ActorKind, Brain, MemoryType
from boltzmann.blocks import Producer
from boltzmann.ingest import Candidate, CandidateSet, RegistrationRequest
from boltzmann.store import MemoryBlockStore

from gaussia_brains_mcp_server.config import Settings
from gaussia_brains_mcp_server.embeddings import (
    EmbeddingsError,
    OpenAICompatibleEmbeddings,
)
from gaussia_brains_mcp_server.indices import EngineIndexProvider
from gaussia_brains_mcp_server.indices.engines import ENV, VectorIndex
from gaussia_brains_mcp_server.search import SearchInputError, search_brain
from gaussia_brains_mcp_server.text import fold, tokenize

ANA = Actor(id="ana@example.com", kind=ActorKind.HUMAN)


def test_tokenize_and_fold_match_core() -> None:
    assert tokenize("Él comió_pan, a 2x") == ["él", "comió", "pan", "2x"]
    assert fold("comió") == "comio"


class KeywordEmbeddings:
    """Deterministic 3-d embeddings: one axis per topic word."""

    model_tag = "test/keywords"

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        out = []
        for text in texts:
            low = text.lower()
            out.append(
                [
                    1.0 if "cat" in low or "feline" in low else 0.0,
                    1.0 if "rocket" in low or "space" in low else 0.0,
                    0.1,
                ]
            )
        return out


@pytest.fixture
def embeddings():
    previous = ENV.embeddings
    ENV.embeddings = KeywordEmbeddings()
    yield ENV.embeddings
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


def _seed(brain: Brain, facts: list[tuple[str, str]]) -> dict[str, str]:
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
                    payload={"kind": "fact", "label": label, "statement": text},
                    evidence=[source.block_id],
                )
                for label, text in facts
            ],
        ),
        task,
    )
    brain.commit(report)
    return {r.candidate.payload["label"]: str(r.block.block_id) for r in report.results}


def test_lexical_search_ranks_term_matches() -> None:
    brain = _brain()
    ids = _seed(
        brain,
        [
            ("pasta", "Carbonara uses guanciale and pecorino."),
            ("bread", "Sourdough needs a starter."),
        ],
    )
    found = search_brain(brain, "guanciale")
    assert [r["block_id"] for r in found["results"]][0] == ids["pasta"]
    assert found["narrowing"]["semantic"] is False


def test_semantic_search_uses_the_vector_index(embeddings) -> None:
    brain = _brain()
    ids = _seed(
        brain,
        [("cats", "The cat sleeps all day."), ("space", "The rocket reached orbit.")],
    )
    found = search_brain(
        brain, "tell me about feline behaviour please", mode="semantic"
    )
    assert found["results"][0]["block_id"] == ids["cats"]
    assert found["narrowing"]["semantic"] is True


def test_vector_index_refuses_another_models_vectors(embeddings) -> None:
    index = VectorIndex()
    index.load(
        json.dumps({"version": 1, "model_tag": "other/model", "vectors": {}}).encode()
    )
    assert VectorIndex.accepts("other/model") is False
    assert VectorIndex.accepts("test/keywords") is True
    assert index.search("cat") == []


def test_search_input_validation() -> None:
    brain = _brain()
    with pytest.raises(SearchInputError):
        search_brain(brain, "x", limit=0)
    with pytest.raises(SearchInputError):
        search_brain(brain, "x", mode="psychic")
    with pytest.raises(SearchInputError):
        search_brain(brain, "x", memory_types=["dreams"])


def test_openai_compatible_client_batches_and_orders() -> None:
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        assert request.headers["authorization"] == "Bearer k"
        data = [
            {"index": i, "embedding": [float(len(t)), 0.0]}
            for i, t in enumerate(body["input"])
        ]
        return httpx.Response(200, json={"data": list(reversed(data))})

    client = OpenAICompatibleEmbeddings(
        "http://emb/v1",
        "m",
        model_tag="openai/m",
        api_key="k",
        batch_size=2,
        transport=httpx.MockTransport(handler),
    )
    vectors = client.embed_documents(["a", "bbb", ""])
    assert vectors == [[1.0, 0.0], [3.0, 0.0], [1.0, 0.0]]
    assert [len(b["input"]) for b in seen] == [2, 1]
    assert seen[0]["model"] == "m"


def test_openai_compatible_client_refuses_short_answers() -> None:
    transport = httpx.MockTransport(lambda _r: httpx.Response(200, json={"data": []}))
    client = OpenAICompatibleEmbeddings(
        "http://emb/v1", "m", model_tag="t", transport=transport
    )
    with pytest.raises(EmbeddingsError):
        client.embed_documents(["x"])


def test_model_tag_defaults() -> None:
    assert Settings(
        embeddings_model="text-embedding-3-small"
    ).effective_embeddings_model_tag() == ("openai/text-embedding-3-small")
    assert (
        Settings(embeddings_model="baai/bge-m3").effective_embeddings_model_tag()
        == "baai/bge-m3"
    )
    assert Settings().effective_embeddings_model_tag() is None
