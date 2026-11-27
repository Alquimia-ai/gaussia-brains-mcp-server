"""Long operations as jobs: push, pull, converge and import.

A request that starts one answers ``202`` with the job; ``GET /v1/jobs/{id}``
reports it and ``POST /v1/jobs/{id}/cancel`` stops it.

Cancelling is allowed until the job reaches its point of no return, which the
job declares with :meth:`JobContext.commit_point`: for a push, the moment
before the manifest is written (blobs already uploaded are harmless, a moved
tag is not); for a pull, import or converge, the moment before the brain's head
pointer moves. After that a cancel is refused with ``usage`` and the job runs
to the end.

Cancelling is cooperative with work running in worker threads: the job's
cancel flag and its commit point are guarded by one thread lock, so a cancel is
either accepted before the commit point (and the worker stops at its next
checkpoint) or refused after it, whichever thread gets there first.

Records are durable on the writer replica: every job is written to
``<GBMS_STORE_ROOT>/.jobs/<id>.json`` (atomically) when it is submitted, when
its state or stage changes and when it finishes; the oldest finished records
are dropped past ``GBMS_JOBS_MAX_RETAINED``. A read-only replica keeps jobs in
memory only. On startup the records are loaded back, and a job that had not
finished is reported ``failed`` at stage ``interrupted`` with error code
``interrupted``, whose ``detail.passed_commit_point`` says whether it may have
changed anything (no job writes anything it has not finished: blobs are
content-addressed and pointers move last, so before its commit point nothing
was changed).

On shutdown (:meth:`JobRunner.aclose`) new jobs are refused with
``registry_unavailable`` (``detail.restarting``), jobs still before their commit
point are cancelled, and jobs past it get up to ``GBMS_SHUTDOWN_GRACE_SECONDS``
to finish before they are cancelled too.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import json
import os
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from loguru import logger

from gaussia_brains_mcp_server.lifecycle.errors import (
    NOT_FOUND,
    REGISTRY_UNAVAILABLE,
    USAGE,
    LifecycleError,
    describe,
)

#: Job error codes that are not HTTP codes: a job's own outcome.
CANCELLED = "cancelled"
INTERRUPTED = "interrupted"

INTERRUPTED_BEFORE = (
    "the service restarted before this job reached its point of no return; "
    "nothing was changed — run it again"
)
INTERRUPTED_AFTER = (
    "the service restarted after this job's point of no return; it may have "
    "completed — re-read the brain's status before retrying"
)
RESTARTING = "the service is restarting; retry shortly"


def interrupted_error(passed_commit_point: bool) -> dict[str, Any]:
    return {
        "code": INTERRUPTED,
        "message": INTERRUPTED_AFTER if passed_commit_point else INTERRUPTED_BEFORE,
        "detail": {"passed_commit_point": passed_commit_point},
    }


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
    #: Whether the job declared its point of no return (it may have changed
    #: something). Persisted, so a restart can say so.
    passed_commit_point: bool = False
    #: Submission order (``created_at`` has one-second resolution); persisted
    #: so that records load back in the order they were submitted.
    sequence: int = field(default_factory=time.time_ns)
    task: asyncio.Task[Any] | None = field(default=None, repr=False)
    #: Guards ``cancel_requested`` against ``cancellable``: a commit point
    #: may be declared from a worker thread while a cancel arrives on the loop.
    guard: threading.Lock = field(default_factory=threading.Lock, repr=False)

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

    def record(self) -> dict[str, Any]:
        """What is persisted: the view plus what a restart needs to judge it."""
        return {
            **self.view(),
            "passed_commit_point": self.passed_commit_point,
            "cancel_requested": self.cancel_requested,
            "sequence": self.sequence,
        }

    @classmethod
    def from_record(cls, data: dict[str, Any]) -> Job:
        return cls(
            id=str(data["id"]),
            kind=str(data["kind"]),
            brain=str(data["brain"]),
            actor=str(data["actor"]),
            state=JobState(data["state"]),
            stage=str(data.get("stage") or ""),
            cancellable=bool(data.get("cancellable", False)),
            cancel_requested=bool(data.get("cancel_requested", False)),
            created_at=str(data.get("created_at") or _now()),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            params=dict(data.get("params") or {}),
            result=data.get("result"),
            error=data.get("error"),
            passed_commit_point=bool(data.get("passed_commit_point", False)),
            sequence=int(data.get("sequence") or 0),
        )


class JobCancelled(Exception):
    """Raised inside a job whose cancel arrived before its point of no return."""


class JobContext:
    """What a running job uses to report progress and declare its commit point."""

    def __init__(self, job: Job, runner: JobRunner | None = None) -> None:
        self.job = job
        self._runner = runner

    def stage(self, name: str) -> None:
        self.job.stage = name
        if self._runner is not None:
            self._runner.persist(self.job)

    def checkpoint(self) -> None:
        """Stop here if a cancel was requested."""
        if self.job.cancel_requested:
            raise JobCancelled(f"job {self.job.id} was cancelled")

    def commit_point(self, what: str) -> None:
        """From here on the job cannot be cancelled.

        Synchronous on purpose: between this check and the next ``await`` no
        cancel can be accepted, so a cancel either lands before the commit
        point or is refused. The job's guard makes the same hold when the
        commit point is declared from a worker thread.
        """
        with self.job.guard:
            self.checkpoint()
            self.job.cancellable = False
            self.job.passed_commit_point = True
            self.job.stage = what
        if self._runner is not None:
            self._runner.persist(self.job)


_current: contextvars.ContextVar[JobContext | None] = contextvars.ContextVar(
    "gbms_current_job", default=None
)


def current_job() -> JobContext | None:
    """The job the calling coroutine runs in, if any (e.g. for the registry client)."""
    return _current.get()


JobBody = Callable[[JobContext], Awaitable[dict[str, Any]]]
#: Runs when a job ends without succeeding; may be a plain function or a coroutine.
JobCleanup = Callable[[], Awaitable[None] | None]


class JobRunner:
    """Runs jobs as asyncio tasks and keeps a bounded record of them.

    With *directory*, every record is also written there (one JSON file per
    job) and :meth:`recover` loads them back after a restart.
    """

    def __init__(
        self, *, max_retained: int = 500, directory: str | Path | None = None
    ) -> None:
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._max = max_retained
        self._dir = Path(directory) if directory is not None else None
        if self._dir is not None:
            self._dir.mkdir(parents=True, exist_ok=True)
        self._persist_lock = threading.Lock()
        self._closing = False

    # -- persistence ----------------------------------------------------------- #

    @property
    def directory(self) -> Path | None:
        return self._dir

    def _path(self, job_id: str) -> Path:
        assert self._dir is not None
        return self._dir / f"{job_id}.json"

    def persist(self, job: Job) -> None:
        """Write *job*'s record atomically (temp file + rename); no-op in memory."""
        if self._dir is None:
            return
        payload = json.dumps(job.record(), sort_keys=True, default=str)
        try:
            with self._persist_lock:
                fd, scratch = tempfile.mkstemp(
                    prefix=f".tmp-{job.id}-", suffix=".json", dir=self._dir
                )
                try:
                    with os.fdopen(fd, "w") as out:
                        out.write(payload)
                    os.replace(scratch, self._path(job.id))
                except BaseException:
                    with contextlib.suppress(OSError):
                        os.unlink(scratch)
                    raise
        except OSError as exc:  # a full volume must not fail the job itself
            logger.warning("could not persist job {}: {}", job.id, exc)

    def _forget(self, job_id: str) -> None:
        if self._dir is None:
            return
        with contextlib.suppress(OSError):
            self._path(job_id).unlink()

    def recover(self) -> list[Job]:
        """Load persisted records; jobs that had not finished become ``interrupted``.

        Returns the jobs that were interrupted.
        """
        if self._dir is None:
            return []
        for stray in self._dir.glob(".tmp-*"):
            with contextlib.suppress(OSError):
                stray.unlink()
        loaded: list[Job] = []
        for path in self._dir.glob("job_*.json"):
            try:
                loaded.append(Job.from_record(json.loads(path.read_text())))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                logger.warning("unreadable job record {}: {}", path, exc)
        loaded.sort(key=lambda j: (j.created_at, j.sequence, j.id))
        interrupted: list[Job] = []
        for job in loaded:
            if job.id in self._jobs:
                continue
            if job.state not in FINISHED:
                job.state = JobState.FAILED
                job.stage = "interrupted"
                job.cancellable = False
                job.finished_at = _now()
                job.error = interrupted_error(job.passed_commit_point)
                self.persist(job)
                interrupted.append(job)
            self._jobs[job.id] = job
        self._trim()
        if interrupted:
            logger.warning(
                "{} job(s) interrupted by a restart: {}",
                len(interrupted),
                ", ".join(f"{j.id} ({j.kind} {j.brain})" for j in interrupted),
            )
        return interrupted

    # -- running ---------------------------------------------------------------- #

    @property
    def closing(self) -> bool:
        return self._closing

    def ensure_accepting(self) -> None:
        """Refuse new work while shutting down (``registry_unavailable``)."""
        if self._closing:
            raise LifecycleError(
                REGISTRY_UNAVAILABLE, RESTARTING, detail={"restarting": True}
            )

    def submit(
        self,
        kind: str,
        *,
        brain: str,
        actor: str,
        body: JobBody,
        params: dict[str, Any] | None = None,
        cleanup: JobCleanup | None = None,
    ) -> Job:
        """Start *body* as a job.

        *cleanup* runs whenever the job ends without succeeding -- failed,
        cancelled, even cancelled before it started -- e.g. to remove a brain
        an import created and never filled. It may be async; it is awaited
        after the body has returned, so any lock the body held is released.

        Raises:
            LifecycleError: ``registry_unavailable`` while shutting down.
        """
        self.ensure_accepting()
        job = Job(
            id=f"job_{uuid.uuid4().hex}",
            kind=kind,
            brain=brain,
            actor=actor,
            params=params or {},
        )
        self._jobs[job.id] = job
        self._trim()
        self.persist(job)
        job.task = asyncio.get_running_loop().create_task(
            self._run(job, body, cleanup), name=f"gbms-{kind}-{job.id}"
        )
        return job

    async def _run(self, job: Job, body: JobBody, cleanup: JobCleanup | None) -> None:
        ctx = JobContext(job, self)
        token = _current.set(ctx)
        job.state = JobState.RUNNING
        job.started_at = _now()
        job.stage = "running"
        self.persist(job)
        try:
            ctx.checkpoint()
            job.result = await body(ctx)
            job.state = JobState.SUCCEEDED
            job.stage = "done"
        except (asyncio.CancelledError, JobCancelled):
            if job.passed_commit_point:
                # Only a shutdown cancels a job past its commit point, and
                # what it did is not known: say so rather than "cancelled".
                job.state = JobState.FAILED
                job.stage = "interrupted"
                job.error = interrupted_error(True)
            else:
                job.state = JobState.CANCELLED
                job.stage = "cancelled"
                job.error = {
                    "code": CANCELLED,
                    "message": "the job was cancelled because the service is "
                    "restarting; nothing was changed — run it again"
                    if self._closing
                    else "the job was cancelled",
                }
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
            self.persist(job)
            if cleanup is not None and job.state is not JobState.SUCCEEDED:
                try:
                    outcome = cleanup()
                    if inspect.isawaitable(outcome):
                        await outcome
                except Exception:  # noqa: BLE001 - the job's own outcome stands
                    logger.exception("cleanup after job {} failed", job.id)

    def get(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise LifecycleError(NOT_FOUND, f"no job {job_id!r}")
        return job

    def list(self, *, brain: str | None = None, actor: str | None = None) -> list[Job]:
        """Jobs newest first, optionally only *brain*'s and/or only *actor*'s."""
        jobs = [
            j
            for j in self._jobs.values()
            if (brain is None or j.brain == brain)
            and (actor is None or j.actor == actor)
        ]
        return list(reversed(jobs))

    def cancel(self, job_id: str) -> Job:
        job = self.get(job_id)
        if job.state in FINISHED:
            raise LifecycleError(
                USAGE, f"job {job_id} already {job.state.value}", detail=job.view()
            )
        if not self._request_cancel(job):
            raise LifecycleError(
                USAGE,
                f"job {job_id} is past its point of no return ({job.stage}) and "
                f"will run to the end",
                detail=job.view(),
            )
        return job

    def _request_cancel(self, job: Job) -> bool:
        """Cancel *job* if it is before its commit point; whether it was."""
        with job.guard:
            if not job.cancellable:
                return False
            job.cancel_requested = True
        # A task that has not started yet is not cancelled: cancelling it would
        # throw into the coroutine before it records anything, leaving the job
        # "queued" forever. It sees the flag at its first checkpoint instead.
        if job.state is JobState.RUNNING and job.task is not None:
            job.task.cancel()
        return True

    async def aclose(self, grace: float = 0.0, *, settle: float = 5.0) -> None:
        """Shut down: refuse new jobs, cancel what can be, wait for the rest.

        Jobs before their commit point are cancelled at once. Jobs past it are
        waited for up to *grace* seconds, then cancelled; a job that still has
        not ended *settle* seconds later is left as it is, and its persisted
        record (still running) is reported ``interrupted`` on the next start.
        """
        self._closing = True
        active = [j for j in self._jobs.values() if j.task and not j.task.done()]
        if not active:
            return
        for job in active:
            self._request_cancel(job)
        tasks = [j.task for j in active if j.task is not None]
        pending = {t for t in tasks if not t.done()}
        if grace > 0 and pending:
            _, pending = await asyncio.wait(pending, timeout=grace)
        if not pending:
            return
        for task in pending:
            task.cancel()
        _, stuck = await asyncio.wait(pending, timeout=settle)
        if stuck:
            logger.warning(
                "{} job(s) still running at shutdown; they will be reported "
                "interrupted on the next start",
                len(stuck),
            )

    def _trim(self) -> None:
        if len(self._jobs) <= self._max:
            return
        for job_id in [k for k, j in self._jobs.items() if j.state in FINISHED]:
            if len(self._jobs) <= self._max:
                break
            del self._jobs[job_id]
            self._forget(job_id)
