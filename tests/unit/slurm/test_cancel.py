"""Cancellation must target one job owned by the authenticated remote UID."""
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.slurm import cancel_once
from hpc_mcp.slurm.cancel import CancellationStore, SlurmCancelService
from hpc_mcp.project.registry import ProjectRegistry
from hpc_mcp.ssh.protocol import CommandResult


REQUEST = UUID("00000000-0000-4000-8000-000000000001")


@pytest.fixture
def remote(monkeypatch):
    calls = []
    state = SimpleNamespace(queue=(0, "123|1001|RUNNING\n", "", False), cancel=(0, "", "", False))
    monkeypatch.setattr(cancel_once.os, "getuid", lambda: 1001, raising=False)

    async def run(program, args):
        calls.append((program, args))
        return state.queue if program == "squeue" else state.cancel

    monkeypatch.setattr(cancel_once, "run", run)
    return state, calls


async def test_remote_checks_uid_and_passes_one_id_with_owner_filter(remote):
    _, calls = remote
    result = await cancel_once.cancel("123", str(REQUEST))
    assert result["status"] == "cancellation_requested"
    assert result["actor_uid"] == result["owner_uid"] == 1001
    assert calls[-1] == ("scancel", ["--user=1001", "--", "123"])


@pytest.mark.parametrize("job", ["", "0", "123,124", "123-124", "--user=alice", "123;id", "123.batch", "123_[1-5]", "01"])
async def test_invalid_job_never_reaches_scheduler(remote, job):
    _, calls = remote
    with pytest.raises(ValueError):
        await cancel_once.cancel(job, str(REQUEST))
    assert not calls


@pytest.mark.parametrize("queue,code", [
    ((0, "123|1002|RUNNING\n", "", False), "JOB_NOT_OWNED"),
    ((0, "124|1001|RUNNING\n", "", False), "SLURM_QUERY_INVALID"),
    ((0, "123|1001|RUNNING\n", "", True), "SLURM_QUERY_INVALID"),
    ((1, "", "controller unavailable", False), "SLURM_QUERY_FAILED"),
    ((0, "unparseable", "", False), "SLURM_QUERY_INVALID"),
])
async def test_no_cancel_on_unverified_owner_or_query(remote, queue, code):
    state, calls = remote
    state.queue = queue
    result = await cancel_once.cancel("123", str(REQUEST))
    assert result["error_code"] == code
    assert len(calls) == 1


@pytest.mark.parametrize("queue", [(0, "", "", False), (1, "", "slurm_load_jobs error: Invalid job id specified\n", False)])
async def test_absent_job_is_not_claimed_cancelled(remote, queue):
    state, calls = remote
    state.queue = queue
    assert (await cancel_once.cancel("123", str(REQUEST)))["status"] == "not_active"
    assert len(calls) == 1


async def test_array_parent_checks_every_element_before_cancel(remote):
    state, calls = remote
    state.queue = (0, "123_0|1001|PENDING\n123_1|1002|RUNNING\n", "", False)
    assert (await cancel_once.cancel("123", str(REQUEST)))["error_code"] == "JOB_NOT_OWNED"
    assert len(calls) == 1
    state.queue = (0, "123_0|1001|PENDING\n", "", False)
    assert (await cancel_once.cancel("123_0", str(REQUEST)))["status"] == "cancellation_requested"
    assert calls[-1][1][-1] == "123_0"


async def test_cancel_failure_is_uncertain_and_not_retried(remote):
    state, calls = remote
    state.cancel = (1, "", "scheduler error", False)
    assert (await cancel_once.cancel("123", str(REQUEST)))["error_code"] == "CANCELLATION_UNCERTAIN"
    assert len(calls) == 2


@pytest.fixture
def service(tmp_path):
    calls = []
    registry = ProjectRegistry(tmp_path / "projects.sqlite3")
    project = registry.ensure(tmp_path, "cluster", "/work")
    registry.register_job(project.id, "123", None, None)

    class Ssh:
        async def realpath(self, path):
            return "/home/alice"

        async def run(self, command):
            calls.append(command)
            return CommandResult(exit_status=0, stdout=json.dumps({
                "request_id": str(REQUEST), "job_id": "123", "status": "cancellation_requested",
                "actor_uid": 1001, "owner_uid": 1001, "previous_states": ["RUNNING"], "error_code": None,
            }), stderr="")

    store = CancellationStore(tmp_path / "cancellations.sqlite3")
    return SlurmCancelService(registry, lambda alias: Ssh(), store), store, calls


async def test_durable_request_replay_never_dispatches_twice(service):
    svc, store, calls = service
    first = await svc.cancel_job("123", REQUEST)
    assert first["status"] == "cancellation_requested"
    assert await svc.cancel_job("123", REQUEST) == first
    assert len(calls) == 1
    assert store.get(str(REQUEST))["response"] == first
    assert calls[0].standard_input == Path(cancel_once.__file__).read_bytes()
    with pytest.raises(HpcMcpError, match="different target"):
        await svc.cancel_job("124", REQUEST, "cluster")


async def test_unknown_transport_outcome_is_fenced(service):
    svc, store, calls = service
    async def broken(command):
        calls.append(command)
        raise HpcMcpError("SSH_TIMEOUT", "timeout")
    async def home(path):
        return "/home/alice"
    svc._ssh = lambda alias: SimpleNamespace(realpath=home, run=broken)
    for _ in range(2):
        with pytest.raises(HpcMcpError) as caught:
            await svc.cancel_job("123", REQUEST)
        assert caught.value.code == "CANCELLATION_UNCERTAIN"
        assert not caught.value.retriable
    assert len(calls) == 1
    assert store.get(str(REQUEST))["response"]["status"] == "uncertain"


async def test_audit_failure_prevents_mutation(service, monkeypatch):
    svc, store, calls = service
    def broken(*args):
        raise OSError("audit unavailable")
    monkeypatch.setattr(store, "begin", broken)
    with pytest.raises(HpcMcpError) as caught:
        await svc.cancel_job("123", REQUEST)
    assert caught.value.code == "CANCELLATION_AUDIT_FAILED"
    assert not calls


async def test_rejected_owner_is_audited_and_replayed_without_cancel(service):
    svc, store, calls = service
    async def rejected(command):
        calls.append(command)
        return CommandResult(exit_status=0, stdout=json.dumps({
            "request_id": str(REQUEST), "job_id": "123", "status": "rejected",
            "actor_uid": 1001, "owner_uid": None, "previous_states": [], "error_code": "JOB_NOT_OWNED",
        }), stderr="")
    class Client:
        async def realpath(self, path):
            return "/home/alice"
        run = staticmethod(rejected)
    svc._ssh = lambda alias: Client()
    for _ in range(2):
        with pytest.raises(HpcMcpError) as caught:
            await svc.cancel_job("123", REQUEST)
        assert caught.value.code == "JOB_NOT_OWNED"
    assert len(calls) == 1
    assert store.get(str(REQUEST))["response"]["actor_uid"] == 1001


async def test_pending_intent_and_failed_result_save_fence_retries(service, monkeypatch):
    svc, store, calls = service
    def broken(*args):
        raise OSError("disk unavailable")
    monkeypatch.setattr(store, "finish", broken)
    for _ in range(2):
        with pytest.raises(HpcMcpError) as caught:
            await svc.cancel_job("123", REQUEST)
        assert caught.value.code == "CANCELLATION_UNCERTAIN"
    assert len(calls) == 1
    assert store.get(str(REQUEST))["response"] is None


async def test_invalid_local_target_and_unknown_job_do_not_dispatch(service):
    svc, _, calls = service
    for job, expected in [("--all", "INVALID_CANCELLATION_REQUEST"), ("124", "JOB_NOT_REGISTERED")]:
        with pytest.raises(HpcMcpError) as caught:
            await svc.cancel_job(job, REQUEST)
        assert caught.value.code == expected
    assert not calls


@pytest.mark.parametrize("program", ["squeue", "scancel"])
async def test_missing_program_does_not_claim_success(remote, monkeypatch, program):
    state, calls = remote
    original = cancel_once.run
    async def missing(name, args):
        if name == program:
            raise FileNotFoundError(name)
        return await original(name, args)
    monkeypatch.setattr(cancel_once, "run", missing)
    expected = "SLURM_QUERY_FAILED" if program == "squeue" else "CANCELLATION_UNCERTAIN"
    assert (await cancel_once.cancel("123", str(REQUEST)))["error_code"] == expected


async def test_root_cannot_cancel_another_users_job(remote, monkeypatch):
    _, calls = remote
    monkeypatch.setattr(cancel_once.os, "getuid", lambda: 0)
    assert (await cancel_once.cancel("123", str(REQUEST)))["error_code"] == "JOB_NOT_OWNED"
    assert len(calls) == 1


async def test_subprocess_output_is_bounded_and_scheduler_environment_is_clean(monkeypatch):
    import sys
    monkeypatch.setenv("SCANCEL_USER", "other")
    monkeypatch.setenv("SQUEUE_USERS", "other")
    monkeypatch.setenv("SLURM_CLUSTERS", "other")
    code, stdout, stderr, truncated = await cancel_once.run(sys.executable, ["-c",
        "import os; print([x for x in ('SCANCEL_USER','SQUEUE_USERS','SLURM_CLUSTERS') if x in os.environ]); print('x'*70000)"])
    assert code == 0 and stdout.startswith("[]") and len(stdout) == cancel_once.OUTPUT_LIMIT
    assert not stderr and truncated


async def test_subprocess_timeout_kills_child(monkeypatch):
    import sys
    wait_for = cancel_once.asyncio.wait_for
    monkeypatch.setattr(cancel_once.asyncio, "wait_for", lambda task, timeout: wait_for(task, 0.1))
    code, _, _, truncated = await cancel_once.run(sys.executable, ["-c", "import time; time.sleep(60)"])
    assert code == -1 and truncated
