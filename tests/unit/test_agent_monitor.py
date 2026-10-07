import asyncio
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError


class FakeSlurm:
    def __init__(self):
        self.calls = []
        self.queue = {}
        self.accounting = {}
        self.error = None

    async def query_squeue(self, alias, job_ids):
        self.calls.append(("squeue", alias, job_ids))
        if self.error:
            raise self.error
        return self.queue

    async def query_sacct(self, alias, job_ids):
        self.calls.append(("sacct", alias, job_ids))
        return self.accounting


def register(store, job="123", alias="cluster"):
    return store.register_watch(server_alias=alias, job_id=job, project_id="p",
                                thread_id="t", session_id="s", cwd="/project")


@pytest.mark.asyncio
async def test_100_watches_batched_and_accounting_delay_survives_restart(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    path = tmp_path / "agent.sqlite3"
    store, io = AgentStore(path), FakeSlurm()
    for index in range(100):
        register(store, str(index))
    io.queue = {str(i): SimpleNamespace(state="RUNNING") for i in range(50)}
    await Monitor(store, io, lambda: "v1").poll_once(now=1)
    assert len(io.calls) == 2
    assert len(io.calls[0][2]) == 100
    assert len(io.calls[1][2]) == 50
    assert len([w for w in store.watches() if w.last_state == "VERIFYING"]) == 50
    assert not store.events()
    io.calls.clear()
    io.accounting = {str(i): SimpleNamespace(state="COMPLETED") for i in range(50, 100)}
    await Monitor(AgentStore(path), io, lambda: "v1").poll_once(now=60)
    assert len(store.events()) == 50
    assert len(store.watches(active=True)) == 50
    await Monitor(store, io, lambda: "v1").poll_once(now=120)
    assert len(store.events()) == 50


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["COMPLETED", "FAILED", "CANCELLED by 42", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "PREEMPTED"])
async def test_arrays_and_all_terminal_states_require_accounting(tmp_path, state):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    store, io = AgentStore(tmp_path / "db"), FakeSlurm()
    register(store, "123_7")
    io.queue = {"123_7": SimpleNamespace(state=state)}
    monitor = Monitor(store, io, lambda: "v1")
    await monitor.poll_once(now=1)
    assert not store.events()
    io.queue = {}
    io.accounting = {"123_7": SimpleNamespace(state=state)}
    await monitor.poll_once(now=2)
    assert store.events()[0].terminal_state == state.split()[0]


@pytest.mark.asyncio
async def test_persisted_backoff_and_auth_pause_config_change_retry(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    path = tmp_path / "db"
    store, io = AgentStore(path), FakeSlurm()
    register(store)
    io.error = HpcMcpError("SSH_TRANSPORT_FAILED", "safe")
    now = 0
    for delay in (60, 120, 240, 480, 600, 600):
        await Monitor(AgentStore(path), io, lambda: "v1").poll_once(now=now)
        status = store.server_states()[0]
        assert status.next_retry_at == now + delay
        count = len(io.calls)
        await Monitor(store, io, lambda: "v1").poll_once(now=now + delay - 1)
        assert len(io.calls) == count
        now += delay
    io.error = HpcMcpError("AUTH_FAILED", "safe")
    await Monitor(store, io, lambda: "v1").poll_once(now=now)
    count = len(io.calls)
    await Monitor(AgentStore(path), io, lambda: "v1").poll_once(now=now + 10000)
    assert len(io.calls) == count
    assert store.server_states()[0].paused
    await Monitor(store, io, lambda: "v2").poll_once(now=now + 10001)
    assert len(io.calls) == count + 1
    store.retry_server("cluster")
    io.error = None
    await Monitor(store, io, lambda: "v2").poll_once(now=now + 10002)
    assert len(io.calls) == count + 3
    assert not store.server_states()[0].paused
    assert store.server_states()[0].failures == 0


@pytest.mark.asyncio
async def test_loop_jitter_stop_and_server_isolation(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    store, io = AgentStore(tmp_path / "db"), FakeSlurm()
    register(store, alias="one")
    register(store, alias="two")
    stop = asyncio.Event()
    delays = []
    async def wait(delay):
        delays.append(delay)
        stop.set()
    monitor = Monitor(store, io, lambda: "v1", jitter=lambda: 7, wait=wait)
    await monitor.run(stop)
    assert delays == [52]
    assert [c[1] for c in io.calls] == ["one", "one", "two", "two"]


@pytest.mark.asyncio
async def test_server_failure_does_not_block_other_server_or_repeat_auth(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    store, io = AgentStore(tmp_path / "db"), FakeSlurm()
    register(store, alias="one")
    register(store, alias="two")
    async def queue(alias, ids):
        io.calls.append(("squeue", alias, ids))
        if alias == "one":
            raise HpcMcpError("AUTH_FAILED", "safe")
        return {}
    io.query_squeue = queue
    io.accounting = {"123": SimpleNamespace(state="COMPLETED")}
    monitor = Monitor(store, io, lambda: "v1")
    await monitor.poll_once(now=0)
    await monitor.poll_once(now=1000)
    assert [(c[0], c[1]) for c in io.calls] == [("squeue", "one"), ("squeue", "two"), ("sacct", "two")]
    assert len(store.events()) == 1


@pytest.mark.asyncio
async def test_raw_transport_errors_are_redacted_and_backed_off(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    store, io = AgentStore(tmp_path / "db"), FakeSlurm()
    register(store)
    io.error = OSError("untrusted transport diagnostics")
    await Monitor(store, io, lambda: "v1").poll_once(now=0)
    assert store.server_states()[0].last_error == "SSH_TRANSPORT_FAILED"
    assert store.server_states()[0].next_retry_at == 60


@pytest.mark.asyncio
async def test_stop_interrupts_default_wait_without_sleeping_45_seconds(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    stop = asyncio.Event()
    class StopSlurm(FakeSlurm):
        async def query_squeue(self, alias, ids):
            stop.set()
            return {}
    store = AgentStore(tmp_path / "db")
    register(store)
    await asyncio.wait_for(Monitor(store, StopSlurm(), lambda: "v1").run(stop), timeout=2)


@pytest.mark.asyncio
async def test_delivery_runs_after_poll_even_if_local_config_is_temporarily_unreadable(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.monitor import Monitor
    stop, calls = asyncio.Event(), []
    def bad_config():
        calls.append("poll")
        raise HpcMcpError("CONFIG_READ_FAILED", "safe")
    async def deliver():
        calls.append("deliver")
        stop.set()
    await Monitor(AgentStore(tmp_path / "db"), FakeSlurm(), bad_config, after_poll=deliver).run(stop)
    assert calls == ["poll", "deliver"]
