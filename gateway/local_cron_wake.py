"""Fixed local generic-intake admission. No caller-selected job or execution options.

The control socket supplies same-UID authentication. This is cooperative account
isolation, not a sandbox against another process running as the account owner.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from pathlib import Path

from gateway.platforms.api_server import _reserve_pending_api_work, _release_pending_api_work

JOB_ID = "a7db03368269"
logger = logging.getLogger(__name__)


async def _owned_thread(fn):
    # Cancelling an asyncio waiter cannot stop its thread. Retain drain ownership
    # until that thread really finishes, even after a transport disconnect.
    task = asyncio.create_task(asyncio.to_thread(fn))
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


class LocalCronWake:
    def __init__(self, runner, home: Path, loop):
        self.runner, self.home, self.loop = runner, Path(home), loop
        self._pending_agent_requests = 0
        self.tasks = set()

    def handle(self, params):
        future = asyncio.run_coroutine_threadsafe(self.admit(params), self.loop)
        try:
            return future.result(timeout=2)
        except concurrent.futures.TimeoutError:
            # Never cancel an admission which may already own a durable claim.
            # The outbox retries and the native claim prevents overlapping work.
            return {"status": "pending"}

    def _claim(self):
        from cron.scheduler_provider import InProcessCronScheduler, _profile_cron_scope
        from agent.estop import check_paused
        with _profile_cron_scope(self.home):
            if check_paused("local-cron-wake", logger):
                return None, None
            provider = InProcessCronScheduler()
            job = provider.claim_fire(JOB_ID, force=False, manual=True)
            if job is not None:
                # Ephemeral snapshot only; persisted schedules/prompt/monitor are untouched.
                job["script"] = "github-generic-event.py"
                job["monitor_script"] = None
                job["monitor_url"] = None
            return provider, job

    async def _execute(self, provider, job):
        def run():
            from cron.scheduler_provider import _profile_cron_scope
            from gateway.run import _profile_runtime_scope
            with _profile_runtime_scope(self.home), _profile_cron_scope(self.home):
                provider.fire_claimed(job, adapters=self.runner.adapters, loop=self.loop)
        await _owned_thread(run)

    def _done(self, task, reservation):
        self.tasks.discard(task)
        _release_pending_api_work(self, reservation)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Local cron wake execution failed", exc_info=task.exception())

    async def admit(self, params):
        from gateway.drain_control import drain_requested
        if params:
            return {"status": "refused", "reason": "fixed target; parameters forbidden"}
        # No await between drain gate and reservation: both run on the gateway loop,
        # the same serialization boundary used by API fire and the drain watcher.
        if self.runner._draining or self.runner._external_drain_active or drain_requested(home=self.home):
            return {"status": "draining"}
        if self._pending_agent_requests:
            return {"status": "busy"}
        with _reserve_pending_api_work(self) as reservation:
            try:
                provider, job = await _owned_thread(self._claim)
                if job is None:
                    return {"status": "busy_or_paused"}
                task = asyncio.create_task(self._execute(provider, job))
                self.tasks.add(task)
                reservation["detached"] = True
                task.add_done_callback(lambda done: self._done(done, reservation))
                return {"status": "accepted", "job_id": JOB_ID, "execution_id": job["execution_id"]}
            except Exception:
                logger.exception("Local cron wake admission failed")
                return {"status": "unavailable"}
