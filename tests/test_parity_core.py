"""Top-k parity with core's planner and engines on a fixture brain.

The same fixture brain is built twice: once here with this service's engines,
once in alquimia-core's own environment (a subprocess, since core pulls in its
whole runtime) with ``alquimia.boltzmann.indices``. Both run without
embeddings -- core can only embed through its YAML-configured connector -- so
parity covers the structural indices and the RRF fusion.

Skipped unless core's interpreter is found: ``GBMS_CORE_PYTHON``, else
``../alquimia-core/.venv/bin/python`` next to this repository.

Queries are chosen where this service does not diverge on purpose: no
range-shaped queries (see the planner's module docstring) and no
``expand_depth`` (core registers no graph index).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE_PYTHON = Path(
    os.environ.get("GBMS_CORE_PYTHON")
    or ROOT.parent / "alquimia-core" / ".venv" / "bin" / "python"
)

QUERIES = [
    ("guanciale", "auto"),
    ("sourdough starter", "auto"),
    ("orbit", "auto"),
    ("how do rockets reach orbit around the earth", "auto"),
    ("cats sleep", "lexical"),
    ("comió", "auto"),
    ("pan", "auto"),
]

FACTS = [
    "Carbonara uses guanciale and pecorino.",
    "Amatriciana uses guanciale, tomato and pecorino.",
    "Sourdough needs a starter fed with flour and water.",
    "A sourdough starter doubles in size when it is active.",
    "The rocket reached orbit after eight minutes.",
    "Rockets need escape velocity to leave the earth.",
    "The cat sleeps sixteen hours a day.",
    "Cats and dogs sleep in short cycles.",
    "Él comió pan con manteca.",
    "El pan de masa madre lleva harina, agua y sal.",
] + [f"Guanciale note {i}: cured pork cheek." for i in range(14)]

SCRIPT = r"""
import json, sys
flavor, facts, queries = sys.argv[1], json.loads(sys.argv[2]), json.loads(sys.argv[3])
from boltzmann import Actor, ActorKind, Brain, MemoryType
from boltzmann.blocks import Producer
from boltzmann.ingest import Candidate, CandidateSet, RegistrationRequest
from boltzmann.query import Query, QueryHints, RetrievalMode
from boltzmann.store import MemoryBlockStore
if flavor == "core":
    from alquimia.boltzmann.indices import (
        BTreeIndex, HashMapIndex, HybridQueryPlanner, InvertedIndex,
    )
    indices = {mt: [HashMapIndex(), BTreeIndex(), InvertedIndex()] for mt in MemoryType}
    planner = HybridQueryPlanner(indices)
else:
    from gaussia_brains_mcp_server.indices import EngineIndexProvider
    from gaussia_brains_mcp_server.indices.engines import ENV
    ENV.embeddings = None
    provider = EngineIndexProvider()
    indices = provider.indices()
    planner = provider.planner(indices)
ana = Actor(id="ana@example.com", kind=ActorKind.HUMAN)
brain = Brain(MemoryBlockStore(), actor=ana, planner=planner, indices=indices)
source = brain.register(b"facts", RegistrationRequest(media_type="text/plain", actor=ana))
task = brain.define_task(source.block_id, allowed=[MemoryType.SEMANTIC])
report = brain.validate(CandidateSet(
    producer=Producer(kind="model", id="openai/gpt-4.1"),
    candidates=[Candidate(memory_type="semantic",
                          payload={"kind": "fact", "label": f"f{i}", "statement": t},
                          evidence=[source.block_id]) for i, t in enumerate(facts)],
), task)
brain.commit(report)
out = {}
for text, mode in queries:
    bundle = brain.search(Query(text=text, hints=QueryHints(limit=5, mode=RetrievalMode(mode))))
    out[f"{mode}:{text}"] = [
        [m.content.get("label", m.memory_type.value), m.score] for m in bundle.matches
    ]
print(json.dumps(out))
"""


def _run(python: str, flavor: str) -> dict[str, list[list[str]]]:
    done = subprocess.run(
        [python, "-c", SCRIPT, flavor, json.dumps(FACTS), json.dumps(QUERIES)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=ROOT,
        env={**os.environ, "LOGURU_LEVEL": "ERROR"},
    )
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


@pytest.mark.skipif(
    not CORE_PYTHON.exists(), reason="alquimia-core environment not found"
)
def test_top_k_matches_core() -> None:
    probe = subprocess.run(
        [str(CORE_PYTHON), "-c", "import alquimia.boltzmann.indices"],
        capture_output=True,
        timeout=60,
    )
    if probe.returncode != 0:
        pytest.skip(
            "alquimia.boltzmann.indices is not importable in core's environment"
        )
    core = _run(str(CORE_PYTHON), "core")
    ours = _run(sys.executable, "gbms")
    assert ours == core
