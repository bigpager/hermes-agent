"""Local wake uses real temporary native claims; never starts a real agent."""
import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_local_wake_reserves_before_claim_and_respects_drain(tmp_path):
    from gateway import local_cron_wake as wake
    from cron import jobs
    from cron.scheduler_provider import InProcessCronScheduler
    from gateway.drain_control import write_drain_request, clear_drain_request

    runner = SimpleNamespace(_draining=False, _external_drain_active=False, adapters={})
    gate = wake.LocalCronWake(runner, tmp_path, asyncio.get_running_loop())
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([{'id': wake.JOB_ID, 'name': 'offline', 'prompt': 'offline only',
                        'enabled': True, 'state': 'scheduled',
                        'schedule': {'kind': 'interval', 'minutes': 15},
                        'next_run_at': '2099-01-01T00:00:00+00:00'}])
    write_drain_request(home=tmp_path)
    assert (await gate.admit({}))['status'] == 'draining'
    assert gate._pending_agent_requests == 0
    clear_drain_request(home=tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()
    original = InProcessCronScheduler.claim_fire
    def claim(provider, *args, **kwargs):
        assert gate._pending_agent_requests == 1
        runner._external_drain_active = True
        return original(provider, *args, **kwargs)
    calls = []
    async def execute(provider, job):
        calls.append(job)
        entered.set()
        await release.wait()
    with patch.object(InProcessCronScheduler, 'claim_fire', claim), patch.object(gate, '_execute', execute):
        result = await gate.admit({})
        await asyncio.wait_for(entered.wait(), 5)
        assert result['status'] == 'accepted' and result['execution_id']
        assert gate._pending_agent_requests == 1
        assert (await gate.admit({}))['status'] == 'draining'
        assert calls[0]['script'] == 'github-generic-event.py'
        from cron.monitor import job_has_monitor
        assert not job_has_monitor(calls[0])
        release.set()
        await asyncio.gather(*gate.tasks)
        assert gate._pending_agent_requests == 0


@pytest.mark.asyncio
async def test_socket_peer_and_fixed_target(tmp_path):
    import json, os
    from gateway.control_socket import GatewayControlServer, query_gateway_control
    server = GatewayControlServer(tmp_path, verb_handlers={'wake-generic-intake': lambda params: {'status': 'accepted'}})
    # No transport identity must fail closed, including the named-pipe path.
    request = b'{"verb":"wake-generic-intake"}'
    assert json.loads(server.handle_request_line(request))['ok'] is False
    assert json.loads(server.handle_request_line(request, peer_uid=os.getuid()+1))['ok'] is False
    assert await server.start()
    try:
        result = await asyncio.to_thread(query_gateway_control, tmp_path, 'wake-generic-intake')
        assert result == {'status': 'accepted'}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_paused_busy_failure_and_unknown_target(tmp_path):
    from gateway.local_cron_wake import LocalCronWake, JOB_ID
    from cron import jobs
    from cron.scheduler_provider import InProcessCronScheduler
    from gateway.run_shutdown import GatewayShutdownMixin
    runner = SimpleNamespace(_draining=False, _external_drain_active=False, adapters={})
    gate = LocalCronWake(runner, tmp_path, asyncio.get_running_loop())
    runner._local_cron_wake = gate
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([{'id': JOB_ID, 'name': 'offline', 'prompt': 'offline',
                        'enabled': False, 'state': 'paused', 'paused_at': 'operator',
                        'schedule': {'kind': 'interval', 'minutes': 15},
                        'next_run_at': '2099-01-01T00:00:00+00:00'}])
        before = jobs.get_job(JOB_ID)
        assert (await gate.admit({'job_id': 'unknown'}))['status'] == 'refused'
        assert (await gate.admit({}))['status'] == 'busy_or_paused'
        assert jobs.get_job(JOB_ID) == before
        jobs.update_job(JOB_ID, {'enabled': True, 'state': 'scheduled', 'paused_at': None})
        InProcessCronScheduler().claim_fire(JOB_ID, manual=True)
        assert (await gate.admit({}))['status'] == 'busy_or_paused'
    assert gate._pending_agent_requests == 0
    with patch.object(gate, '_claim', side_effect=OSError('offline fault')):
        assert (await gate.admit({}))['status'] == 'unavailable'
        assert gate._pending_agent_requests == 0
    gate._pending_agent_requests = 1
    assert GatewayShutdownMixin._local_wake_work_count(runner) == 1


@pytest.mark.asyncio
async def test_transport_cancel_cannot_release_live_claim_reservation(tmp_path):
    import threading
    from gateway.local_cron_wake import LocalCronWake
    runner = SimpleNamespace(_draining=False, _external_drain_active=False, adapters={})
    gate = LocalCronWake(runner, tmp_path, asyncio.get_running_loop())
    entered, release = threading.Event(), threading.Event()
    def claim():
        entered.set()
        release.wait(5)
        return None, None
    with patch.object(gate, '_claim', claim):
        task = asyncio.create_task(gate.admit({}))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert gate._pending_agent_requests == 1
        release.set()
        await task
        assert gate._pending_agent_requests == 0


@pytest.mark.asyncio
async def test_real_native_execution_seam_is_profile_bound(tmp_path):
    from gateway.local_cron_wake import LocalCronWake, JOB_ID
    from cron import jobs
    from hermes_constants import get_hermes_home
    runner = SimpleNamespace(_draining=False, _external_drain_active=False, adapters={})
    gate = LocalCronWake(runner, tmp_path, asyncio.get_running_loop())
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([{'id': JOB_ID, 'name': 'offline', 'prompt': 'offline',
                        'enabled': True, 'state': 'scheduled', 'monitor_script': 'must-not-run.py',
                        'schedule': {'kind': 'interval', 'minutes': 15},
                        'next_run_at': '2099-01-01T00:00:00+00:00'}])
    calls=[]
    def native(job, **kwargs):
        assert get_hermes_home() == tmp_path
        assert job['script'] == 'github-generic-event.py'
        assert job['monitor_script'] is None
        calls.append(job['execution_id'])
        return True
    with patch('cron.scheduler.run_one_job', native):
        result=await gate.admit({})
        await asyncio.gather(*gate.tasks)
    assert calls == [result['execution_id']]
    assert gate._pending_agent_requests == 0
