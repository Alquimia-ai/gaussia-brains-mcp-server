"""Evidence and commit in one write: nothing is left behind on a rejection."""

from __future__ import annotations

import base64
from typing import Any

from boltzmann import Brain
from boltzmann.brain import HEAD_POINTER, REFS_POINTER
from boltzmann.ingest import RegistrationRequest
from boltzmann.store import OciLayoutStore
from fastapi.testclient import TestClient

from gaussia_brains_mcp_server.lifecycle.service import (
    canonical_block_for,
    extraction_task,
)
from test_lifecycle import AUTH, REF, _commit, _create, _evidence, _fact, env  # noqa: F401

PRODUCER = {"kind": "model", "id": "openai/gpt-4.1"}


def _inline(text: str, media_type: str = "text/plain") -> dict[str, Any]:
    return {
        "data_base64": base64.b64encode(text.encode()).decode(),
        "media_type": media_type,
        "filename": "policy.txt",
        "origin": None,
    }


def _post(
    client: TestClient, body: dict[str, Any], headers: dict[str, str] | None = None
):
    return client.post(
        f"/v1/brains/{REF}/commits",
        json={"producer": PRODUCER, **body},
        headers={**AUTH, **(headers or {})},
    )


def _state(client: TestClient) -> dict[str, Any]:
    """Everything a write could change: pointers, blobs, history, canonical count."""
    handle = client.app.state.lifecycle.store.get(REF)
    store = handle.store()
    blobs = sorted(p.name for p in (handle.layout / "blobs").rglob("*") if p.is_file())
    status = client.get(f"/v1/brains/{REF}", headers=AUTH).json()
    return {
        "head": store.read_pointer(HEAD_POINTER),
        "refs": store.read_pointer(REFS_POINTER),
        "blobs": blobs,
        "canonical": status["modules"].get("canonical", {}).get("block_count", 0),
    }


def test_rejection_and_dry_run_leave_the_brain_untouched(env) -> None:  # noqa: F811
    client, *_ = env
    _create(client)
    held = _evidence(client, "Held.")["block_id"]
    before = _state(client)

    ghost = "sha256:" + "0" * 64
    rejected = _post(
        client,
        {
            "evidence_inline": _inline("Paris is the capital of France."),
            "candidates": [
                _fact("$source", "paris", "Paris."),
                _fact(ghost, "ghost", "cites nothing held"),
            ],
        },
    )
    assert rejected.status_code == 422, rejected.text
    body = rejected.json()
    assert body["code"] == "rejected_candidates"
    statuses = [r["status"] for r in body["detail"]["report"]["results"]]
    assert statuses[0] != "rejected" and statuses[1] == "rejected"
    assert _state(client) == before

    dry = _post(
        client,
        {
            "evidence_inline": _inline("Paris is the capital of France."),
            "candidates": [_fact("$source", "paris", "Paris.")],
            "dry_run": True,
        },
    )
    assert dry.status_code == 200, dry.text
    assert dry.json()["dry_run"] is True and dry.json()["report"]["clean"] is True
    assert dry.json()["evidence_duplicate"] is False
    assert _state(client) == before
    assert held  # the brain's own evidence is untouched too


def test_accepted_inline_commit_registers_then_commits(env) -> None:  # noqa: F811
    client, *_ = env
    _create(client)
    text = "Rome is the capital of Italy."
    accepted = _post(
        client,
        {
            "evidence_inline": _inline(text),
            "candidates": [_fact("$source", "rome", "Rome.")],
        },
    )
    assert accepted.status_code == 201, accepted.text
    body = accepted.json()
    assert body["evidence_duplicate"] is False and body["source"].startswith("sha256:")
    assert body["committed"][0]["memory_type"] == "semantic"
    assert accepted.headers["ETag"] == f'"{body["head"]}"'

    # The same block registration would have made, and it is cited.
    again = _evidence(client, text)
    assert again["block_id"] == body["source"] and again["duplicate"] is True
    block = client.get(
        f"/v1/brains/{REF}/blocks/{body['committed'][0]['block_id']}", headers=AUTH
    ).json()
    assert block["evidence"] == [body["source"]]
    history = client.get(f"/v1/brains/{REF}/history", headers=AUTH).json()
    assert len(history["snapshots"]) == 2  # the registration and the commit

    # Bytes already held: the ordinary path, flagged as a duplicate.
    dup = _post(
        client,
        {
            "evidence_inline": _inline(text),
            "candidates": [_fact("$source", "rome2", "Rome is old.")],
        },
    )
    assert dup.status_code == 201, dup.text
    assert dup.json()["evidence_duplicate"] is True
    assert dup.json()["source"] == body["source"]


def test_if_match_is_checked_before_anything(env) -> None:  # noqa: F811
    client, *_ = env
    _create(client)
    head = _evidence(client, "First.")["head"]
    _evidence(client, "Second.")
    before = _state(client)
    refused = _post(
        client,
        {
            "evidence_inline": _inline("Third."),
            "candidates": [_fact("$source", "t", "T.")],
        },
        headers={"If-Match": f'"{head}"'},
    )
    assert refused.status_code == 409 and refused.json()["code"] == "not_fast_forward"
    assert _state(client) == before


def test_inline_evidence_input_errors(env, monkeypatch) -> None:  # noqa: F811
    client, *_ = env
    _create(client)
    src = _evidence(client, "Held.")["block_id"]
    candidates = [_fact("$source", "x", "X.")]
    both = _post(
        client,
        {"source": src, "evidence_inline": _inline("x"), "candidates": candidates},
    )
    assert both.status_code == 400 and both.json()["code"] == "usage"
    neither = _post(client, {"candidates": candidates})
    assert neither.status_code == 400 and neither.json()["code"] == "usage"
    not_b64 = _post(
        client,
        {
            "evidence_inline": {"data_base64": "***", "media_type": "text/plain"},
            "candidates": candidates,
        },
    )
    assert not_b64.status_code == 400 and not_b64.json()["code"] == "usage"

    # `$source` also works with a registered source.
    assert _commit(client, src, candidates).status_code == 201

    monkeypatch.setenv("GBMS_STORE_MAX_EVIDENCE_BYTES", "8")
    from gaussia_brains_mcp_server.config import get_settings

    get_settings.cache_clear()
    big = _post(
        client, {"evidence_inline": _inline("123456789"), "candidates": candidates}
    )
    assert big.status_code == 413 and big.json()["code"] == "usage"
    fits = _post(
        client, {"evidence_inline": _inline("12345678"), "candidates": candidates}
    )
    assert fits.status_code == 201, fits.text


def test_prospective_block_and_task_match_the_sdk(tmp_path) -> None:
    """Pins the two pyboltzmann behaviours the atomic commit mirrors."""
    from boltzmann import Actor, ActorKind, MemoryType

    actor = Actor(id="ana@example.com", kind=ActorKind.HUMAN)
    brain = Brain(OciLayoutStore(tmp_path / "layout", create=True), actor=actor)
    for data, media in ((b"one", "text/plain"), (b"%PDF two", "application/pdf")):
        request = RegistrationRequest(media_type=media, actor=actor)
        predicted = canonical_block_for(data, request)
        registered = brain.register(data, request)
        assert registered.block_id == predicted.block_id
        for kwargs in (
            {},
            {
                "allowed": [MemoryType.SEMANTIC],
                "requirements": ["be brief"],
                "instructions": "only facts",
                "task_id": "t1",
            },
        ):
            assert brain.define_task(registered.block_id, **kwargs) == extraction_task(
                registered.block_id,
                allowed=kwargs.get("allowed"),
                requirements=kwargs.get("requirements"),
                instructions=kwargs.get("instructions"),
                task_id=kwargs.get("task_id"),
            )
