"""Long operations as jobs: push, pull, converge and import.

A request that starts one answers ``202`` with the job; ``GET /v1/jobs/{id}``
reports it and ``POST /v1/jobs/{id}/cancel`` stops it.

Cancelling is allowed until the job reaches its point of no return, which the
job declares with :meth:`JobContext.commit_point`: for a push, the moment
before the manifest is written (blobs already uploaded are harmless, a moved
tag is not); for a pull, import or converge, the moment before the brain's head
pointer moves. After that a cancel is refused with ``usage`` and the job runs
to the end.

Jobs live in memory. A restart loses the record of finished jobs and kills
running ones, which is safe because no job writes anything it has not finished:
blobs are content-addressed and pointers move last.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from loguru import logger

from gaussia_brains_mcp_server.lifecycle.errors import (
    NOT_FOUND,
    USAGE,
    LifecycleError,
    describe,
)


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


FINISHED = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class Job:
    id: str
    kind: str
    brain: str
    actor: str
    state: JobState = JobState.QUEUED
    stage: str = "queued"
    cancellable: bool = True
    cancel_requested: bool = False
    created_at: str = field(default_factory=_now)
    started_at: str | None = None
    finished_at: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    task: asyncio.Task[Any] | None = field(default=None, repr=False)

    def view(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "brain": self.brain,
            "actor": self.actor,
            "state": self.state.value,
            "stage": self.stage,
            "cancellable": self.cancellable and self.state not in FINISHED,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "params": self.params,
            "result": self.result,
            "error": self.error,
        }


class JobCancelled(Exception):
    """Raised inside a job whose cancel arrived before its point of no return."""


class JobContext:
    """What a running job uses to report progress and declare its commit point."""

    def __init__(self, job: Job) -> None:
        self.job = job

    def stage(self, name: str) -> None:
        self.job.stage = name

    def checkpoint(self) -> None:
        """Stop here if a cancel was requested."""
        if self.job.cancel_requested:
            raise JobCancelled(f"job {self.job.id} was cancelled")

    def commit_point(self, what: str) -> None:
        """From here on the job cannot be cancelled.

        Synchronous on purpose: between this check and the next ``await`` no
        cancel can be accepted, so a cancel either lands before the commit
        point or is refused.
        """
        self.checkpoint()
        self.job.cancellable = False
        self.job.stage = what


_current: contextvars.ContextVar[JobContext | None] = contextvars.ContextVar(
    "gbms_current_job", default=None
)


def current_job() -> JobContext | None:
    """The job the calling coroutine runs in, if any (e.g. for the registry client)."""
    return _current.get()


JobBody = Callable[[JobContext], Awaitable[dict[str, Any]]]


class JobRunner:
    """Runs jobs as asyncio tasks and keeps a bounded record of them."""

    def __init__(self, *, max_retained: int = 500) -> None:
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._max = max_retained

    def submit(
        self,
        kind: str,
        *,
        brain: str,
        actor: str,
        body: JobBody,
        params: dict[str, Any] | None = None,
        cleanup: Callable[[], None] | None = None,
    ) -> Job:
        """Start *body* as a job.

        *cleanup* runs whenever the job ends without succeeding -- failed,
        cancelled, even cancelled before it started -- e.g. to remove a brain
        an import created and never filled.
        """
        job = Job(
            id=f"job_{uuid.uuid4().hex}",
            kind=kind,
            brain=brain,
            actor=actor,
            params=params or {},
        )
        self._jobs[job.id] = job
        self._trim()
        job.task = asyncio.get_running_loop().create_task(
            self._run(job, body, cleanup), name=f"gbms-{kind}-{job.id}"
        )
        return job

    async def _run(
        self, job: Job, body: JobBody, cleanup: Callable[[], None] | None
    ) -> None:
        ctx = JobContext(job)
        token = _current.set(ctx)
        job.state = JobState.RUNNING
        job.started_at = _now()
        job.stage = "running"
        try:
            ctx.checkpoint()
            job.result = await body(ctx)
            job.state = JobState.SUCCEEDED
            job.stage = "done"
        except (asyncio.CancelledError, JobCancelled):
            job.state = JobState.CANCELLED
            job.stage = "cancelled"
            job.error = {"code": "cancelled", "message": "the job was cancelled"}
        except Exception as exc:  # noqa: BLE001 - reported on the job
            info = describe(exc)
            if info.code == "internal":
                logger.exception("job {} ({}) failed", job.id, job.kind)
            else:
                logger.info("job {} ({}) failed: {}", job.id, job.kind, info.message)
            job.state = JobState.FAILED
            job.stage = "failed"
            job.error = info.body()
            partial = getattr(exc, "result", None)
            if isinstance(partial, dict):
                job.result = partial
        finally:
            job.finished_at = _now()
            job.cancellable = False
            _current.reset(token)
            if cleanup is not None and job.state is not JobState.SUCCEEDED:
                try:
                    cleanup()
                except Exception:  # noqa: BLE001 - the job's own outcome stands
                    logger.exception("cleanup after job {} failed", job.id)

    def get(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise LifecycleError(NOT_FOUND, f"no job {job_id!r}")
        return job

    def list(self, *, brain: str | None = None) -> list[Job]:
        jobs = [j for j in self._jobs.values() if brain is None or j.brain == brain]
        return list(reversed(jobs))

    def cancel(self, job_id: str) -> Job:
        job = self.get(job_id)
        if job.state in FINISHED:
            raise LifecycleError(
                USAGE, f"job {job_id} already {job.state.value}", detail=job.view()
            )
        if not job.cancellable:
            raise LifecycleError(
                USAGE,
                f"job {job_id} is past its point of no return ({job.stage}) and "
                f"will run to the end",
                detail=job.view(),
            )
        job.cancel_requested = True
        # A task that has not started yet is not cancelled: cancelling it would
        # throw into the coroutine before it records anything, leaving the job
        # "queued" forever. It sees the flag at its first checkpoint instead.
        if job.state is JobState.RUNNING and job.task is not None:
            job.task.cancel()
        return job

    async def aclose(self) -> None:
        running = [j.task for j in self._jobs.values() if j.task and not j.task.done()]
        for task in running:
            task.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)

    def _trim(self) -> None:
        if len(self._jobs) <= self._max:
            return
        for job_id in [k for k, j in self._jobs.items() if j.state in FINISHED]:
            if len(self._jobs) <= self._max:
                break
            del self._jobs[job_id]
