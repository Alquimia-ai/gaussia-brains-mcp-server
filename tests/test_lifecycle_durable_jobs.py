"""Job records survive a restart; shutdown is graceful."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from gaussia_brains_mcp_server.lifecycle import errors
from gaussia_brains_mcp_server.lifecycle.jobs import (
    INTERRUPTED_AFTER,
    INTERRUPTED_BEFORE,
    JobRunner,
    JobState,
)
from gaussia_brains_mcp_server.main import create_app
from test_lifecycle import ANA, AUTH, _configure


async def _never(ctx) -> dict[str, Any]:
    await asyncio.sleep(3600)
    return {}


async def test_records_are_persisted_on_every_change(tmp_path: Path) -> None:
    runner = JobRunner(directory=tmp_path)
    step = asyncio.Event()

    async def body(ctx) -> dict[str, Any]:
        ctx.stage("fetching")
        await step.wait()
        ctx.commit_point("installing")
        return {"ok": True}

    job = runner.submit("pull", brain="b", actor="ana@example.com", body=body)
    path = tmp_path / f"{job.id}.json"
    assert json.loads(path.read_text())["state"] == "queued"
    await asyncio.sleep(0.01)
    record = json.loads(path.read_text())
    assert record["state"] == "running" and record["stage"] == "fetching"
    assert record["cancellable"] is True and record["passed_commit_point"] is False
    step.set()
    await job.task
    record = json.loads(path.read_text())
    assert record["state"] == "succeeded" and record["result"] == {"ok": True}
    assert record["passed_commit_point"] is True and record["finished_at"]
    assert not list(tmp_path.glob(".tmp-*"))


async def test_a_restart_reports_unfinished_jobs_interrupted(tmp_path: Path) -> None:
    runner = JobRunner(directory=tmp_path)
    past = asyncio.Event()

    async def committed(ctx) -> dict[str, Any]:
        ctx.commit_point("installing")
        past.set()
        await asyncio.sleep(3600)
        return {}

    before = runner.submit("pull", brain="b", actor="a@example.com", body=_never)
    after = runner.submit("push", brain="b", actor="a@example.com", body=committed)
    done = runner.submit(
        "pull", brain="b", actor="a@example.com", body=AsyncMock(return_value={})
    )
    await past.wait()
    await done.task
    # The process dies here: nothing records the running jobs as finished.
    for job in (before, after):
        assert job.task is not None
        job.task.cancel()
    await asyncio.gather(before.task, after.task, return_exceptions=True)
    for job in (before, after):  # what a killed process leaves on disk
        record = json.loads((tmp_path / f"{job.id}.json").read_text())
        record.update({"state": "running", "finished_at": None, "error": None})
        (tmp_path / f"{job.id}.json").write_text(json.dumps(record))

    restarted = JobRunner(directory=tmp_path)
    interrupted = restarted.recover()
    assert {j.id for j in interrupted} == {before.id, after.id}
    first = restarted.get(before.id)
    assert first.state is JobState.FAILED and first.stage == "interrupted"
    assert first.error == {
        "code": "interrupted",
        "message": INTERRUPTED_BEFORE,
        "detail": {"passed_commit_point": False},
    }
    second = restarted.get(after.id).view()
    assert second["state"] == "failed" and second["cancellable"] is False
    assert second["error"]["message"] == INTERRUPTED_AFTER
    assert second["error"]["detail"] == {"passed_commit_point": True}
    assert restarted.get(done.id).state is JobState.SUCCEEDED
    # The verdict is persisted too.
    on_disk = json.loads((tmp_path / f"{before.id}.json").read_text())
    assert on_disk["stage"] == "interrupted"
    assert [j.id for j in restarted.list()] == [done.id, after.id, before.id]


async def test_retention_deletes_trimmed_records(tmp_path: Path) -> None:
    runner = JobRunner(max_retained=2, directory=tmp_path)
    for _ in range(4):
        job = runner.submit(
            "pull", brain="b", actor="a", body=AsyncMock(return_value={})
        )
        await job.task
    assert len(runner.list()) == 2
    assert sorted(p.stem for p in tmp_path.glob("job_*.json")) == sorted(
        j.id for j in runner.list()
    )


async def test_in_memory_runner_writes_nothing(tmp_path: Path) -> None:
    runner = JobRunner()
    job = runner.submit("pull", brain="b", actor="a", body=AsyncMock(return_value={}))
    await job.task
    assert runner.directory is None and runner.recover() == []


async def test_shutdown_refuses_cancels_and_waits(tmp_path: Path) -> None:
    runner = JobRunner(directory=tmp_path)
    past = threading.Event()
    release = threading.Event()

    def finish(ctx) -> dict[str, Any]:
        ctx.commit_point("writing")
        past.set()
        release.wait(5)
        return {"finished": True}

    async def committed(ctx) -> dict[str, Any]:
        return await asyncio.to_thread(finish, ctx)

    early = runner.submit("pull", brain="b", actor="a", body=_never)
    late = runner.submit("push", brain="b", actor="a", body=committed)
    await asyncio.to_thread(past.wait, 5)

    async def later() -> None:
        await asyncio.sleep(0.1)
        release.set()

    releasing = asyncio.create_task(later())
    await runner.aclose(grace=5)
    await releasing
    assert early.state is JobState.CANCELLED
    assert "restarting" in early.error["message"]
    assert late.state is JobState.SUCCEEDED and late.result == {"finished": True}

    with pytest.raises(errors.LifecycleError) as caught:
        runner.submit("pull", brain="b", actor="a", body=_never)
    assert caught.value.code == "registry_unavailable"
    assert caught.value.status == 502
    assert caught.value.message == "the service is restarting; retry shortly"
    assert caught.value.detail == {"restarting": True}


async def test_shutdown_cancels_past_the_grace_as_interrupted() -> None:
    runner = JobRunner()
    past = asyncio.Event()

    async def committed(ctx) -> dict[str, Any]:
        ctx.commit_point("writing")
        past.set()
        await asyncio.sleep(3600)
        return {}

    job = runner.submit("push", brain="b", actor="a", body=committed)
    await past.wait()
    await runner.aclose(grace=0.05)
    assert job.state is JobState.FAILED and job.stage == "interrupted"
    assert job.error["detail"] == {"passed_commit_point": True}


def test_restart_of_the_app_recovers_jobs(monkeypatch, tmp_path: Path) -> None:
    _configure(monkeypatch, tmp_path)
    jobs_dir = tmp_path / "store" / ".jobs"
    jobs_dir.mkdir(parents=True)
    record = {
        "id": "job_" + "a" * 32,
        "kind": "push",
        "brain": "registry.example.com/acme/handbook",
        "actor": "ana@example.com",
        "state": "running",
        "stage": "pushing",
        "cancellable": True,
        "created_at": "2026-01-01T00:00:00Z",
        "started_at": "2026-01-01T00:00:01Z",
        "finished_at": None,
        "params": {"branch": "main"},
        "result": None,
        "error": None,
        "passed_commit_point": False,
    }
    (jobs_dir / f"{record['id']}.json").write_text(json.dumps(record))
    app = create_app()
    with TestClient(app) as client:
        app.state.token_verifier.verify = AsyncMock(return_value=ANA)
        view = client.get(f"/v1/jobs/{record['id']}", headers=AUTH)
        assert view.status_code == 200, view.text
        body = view.json()
        assert body["state"] == "failed" and body["stage"] == "interrupted"
        assert body["error"]["code"] == "interrupted"
        assert body["error"]["message"] == INTERRUPTED_BEFORE
        listed = client.get("/v1/jobs", headers=AUTH).json()["jobs"]
        assert [j["id"] for j in listed] == [record["id"]]
