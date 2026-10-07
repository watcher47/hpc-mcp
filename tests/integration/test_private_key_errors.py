"""Local invalid key imports must not terminate unrelated agent polling.

All key material is synthetic invalid data; only the AsyncSSH connection
boundary is replaced, so SSH policy, trust, Slurm parsing, and durable monitor
state still execute. No key file, network, or external process is used.
"""
import asyncio
import base64
from contextlib import asynccontextmanager
import json
import stat
import struct
import traceback
from types import SimpleNamespace

import asyncssh
import pytest

from hpc_mcp.agent.monitor import Monitor
from hpc_mcp.agent.store import AgentStore
from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry
from hpc_mcp.slurm.query import SlurmQueryService
from hpc_mcp.ssh.client import SshClient
from hpc_mcp.ssh.policy import RemoteCommand
from hpc_mcp.ssh.trust import HostTrustStore


CONTENT_MARKER = "synthetic-sensitive-key-content"
PASSPHRASE_MARKER = "synthetic-passphrase-value"
KEY_MARKER = "synthetic-private-key-location"


def invalid_key_data(kind):
    if kind == "import":
        # A valid DER NULL inside an unsupported PEM type exercises the actual
        # importer and makes its diagnostic include our synthetic content.
        return (f"-----BEGIN {CONTENT_MARKER} PRIVATE KEY-----\nBQA=\n"
                f"-----END {CONTENT_MARKER} PRIVATE KEY-----\n").encode()

    def string(value):
        return struct.pack(">I", len(value)) + value

    # An invalid OpenSSH encryption header triggers the public decryption
    # exception without generating, reading, or decrypting an actual key.
    packet = (b"openssh-key-v1\0" + string(CONTENT_MARKER.encode())
              + string(b"bcrypt") + string(b"") + struct.pack(">I", 1)
              + string(b"") + string(b""))
    return (b"-----BEGIN OPENSSH PRIVATE KEY-----\n" + base64.b64encode(packet)
            + b"\n-----END OPENSSH PRIVATE KEY-----\n")


def import_failure(kind, passphrase):
    try:
        asyncssh.import_private_key(invalid_key_data(kind), passphrase)
    except (asyncssh.KeyImportError, asyncssh.KeyEncryptionError) as error:
        # Model a dependency diagnostic including other sensitive inputs too.
        error.args = (f"{error}: {KEY_MARKER}: {passphrase}",)
        raise
    pytest.fail("synthetic invalid key was accepted")


class BytesStream:
    def __init__(self, data=b""):
        self.data = data

    async def read(self, size):
        result, self.data = self.data[:size], self.data[size:]
        return result


class Connection:
    def __init__(self, host, commands):
        self.host, self.commands = host, commands

    async def create_process(self, command, **kwargs):
        self.commands.append((self.host, command))
        # A healthy job remains active, so subsequent monitor cycles must
        # continue polling it while the broken alias is durably paused.
        data = b"123|RUNNING|fixture|cpu|00:01|node\n" if "squeue" in command else b""

        async def wait_closed():
            pass

        return SimpleNamespace(stdout=BytesStream(data), stderr=BytesStream(),
                               exit_status=0, wait_closed=wait_closed, close=lambda: None)

    async def start_sftp_client(self):
        return self

    async def realpath(self, path):
        return path

    async def lstat(self, path):
        return SimpleNamespace(permissions=stat.S_IFDIR | 0o755)


@pytest.fixture
def boundary(tmp_path, monkeypatch):
    state = SimpleNamespace(kind="import", phase="enter", broken=True,
                            exception_type=None, attempts=[], commands=[])
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    def connect(host, **options):
        state.attempts.append(host)
        verifier = options["client_factory"]()
        assert verifier.validate_host_public_key(
            host, "192.0.2.20", options["port"],
            SimpleNamespace(get_fingerprint=lambda *args: "SHA256:fixture"))

        def reject():
            if state.broken and host == "broken.invalid":
                if state.exception_type:
                    try:
                        import_failure(state.kind, options["passphrase"])
                    except (asyncssh.KeyImportError, asyncssh.KeyEncryptionError) as error:
                        raise state.exception_type(str(error)) from error
                import_failure(state.kind, options["passphrase"])

        if state.phase == "call":
            reject()

        @asynccontextmanager
        async def context():
            if state.phase == "enter":
                reject()
            yield Connection(host, state.commands)

        return context()

    monkeypatch.setattr(asyncssh, "connect", connect)

    def client(alias="broken"):
        config = ClusterConfig(alias=alias, host=f"{alias}.invalid", username="fixture",
                               private_key=KEY_MARKER, passphrase=PASSPHRASE_MARKER,
                               whitelist=["pwd"])
        return SshClient(config, trust)

    state.client, state.trust = client, trust
    return state


def query_service(tmp_path, boundary):
    registry = ProjectRegistry(tmp_path / "registry.sqlite3")
    for alias in ("broken", "healthy"):
        root = tmp_path / alias
        root.mkdir(exist_ok=True)
        project = registry.ensure(root, alias, "/projects")
        registry.register_job(project.id, "123", "logs/123.out", "logs/123.err")
    return SlurmQueryService(registry, boundary.client, username="fixture")


async def operation(client, name, service=None):
    if name == "run":
        await client.run(RemoteCommand("pwd", [], "/work"))
    elif name == "run_general":
        await client.run_general("pwd", "/work", None)
    elif name == "realpath":
        await client.realpath("/work")
    elif name.startswith("query_"):
        await getattr(service, name)("broken", ["123"])
    else:
        async with client.sftp() as sftp:
            if name != "sftp":
                await getattr(sftp, name)("source", "destination")


def assert_sanitized(error, caplog):
    assert error.code == "AUTH_FAILED"
    assert error.retriable is False
    assert error.details == {}
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = str(error) + repr(vars(error)) + "".join(traceback.format_exception(error)) + caplog.text
    for marker in (CONTENT_MARKER, PASSPHRASE_MARKER, KEY_MARKER,
                   "KeyImportError", "KeyEncryptionError"):
        assert marker not in rendered


@pytest.mark.parametrize("kind", ["import", "encryption"])
@pytest.mark.parametrize("phase", ["call", "enter"])
@pytest.mark.parametrize("name", ["run", "run_general", "sftp", "put", "get", "realpath",
                                  "query_squeue", "query_sacct"])
async def test_private_key_failure_is_sanitized_for_every_connection_user(
        tmp_path, boundary, caplog, kind, phase, name):
    boundary.kind, boundary.phase = kind, phase
    with pytest.raises(HpcMcpError) as caught:
        await operation(boundary.client(), name, query_service(tmp_path, boundary))
    assert_sanitized(caught.value, caplog)
    assert boundary.trust.lookup("broken", "broken.invalid", 22) is None


@pytest.mark.parametrize("kind,base", [("import", asyncssh.KeyImportError),
                                      ("encryption", asyncssh.KeyEncryptionError)])
async def test_private_key_exception_subclasses_are_also_sanitized(boundary, caplog, kind, base):
    class DerivedKeyError(base):
        pass

    boundary.kind, boundary.exception_type = kind, DerivedKeyError
    with pytest.raises(HpcMcpError) as caught:
        await operation(boundary.client(), "run")
    assert_sanitized(caught.value, caplog)
    assert boundary.trust.lookup("broken", "broken.invalid", 22) is None


@pytest.mark.parametrize("phase", ["call", "enter"])
@pytest.mark.parametrize("name", ["run", "run_general", "sftp", "realpath"])
async def test_unrelated_value_error_is_not_normalized(boundary, phase, name):
    boundary.phase, boundary.exception_type = phase, ValueError
    with pytest.raises(ValueError, match="Unknown PEM key type"):
        await operation(boundary.client(), name)
    assert boundary.trust.lookup("broken", "broken.invalid", 22) is None


def monitor_store(tmp_path):
    store = AgentStore(tmp_path / "agent.sqlite3")
    for alias, job in (("broken", "123"), ("healthy", "123"), ("healthy", "456")):
        watch = store.register_watch(server_alias=alias, job_id=job, project_id="fixture",
                                     thread_id="fixture", session_id="fixture", cwd="/project")
        if job == "456":
            store.record_terminal(watch.id, "COMPLETED", now=0)
    return store


@pytest.mark.parametrize("kind", ["import", "encryption"])
async def test_monitor_isolates_key_failure_and_resumes_after_config_change(tmp_path, boundary, kind):
    boundary.kind = kind
    store, service = monitor_store(tmp_path), query_service(tmp_path, boundary)
    pending_event = store.events()[0]
    broken_watch = next(w for w in store.watches() if w.server_alias == "broken")

    await Monitor(store, service, lambda: "v1").poll_once(now=1)
    store = AgentStore(store.path)
    states = {s.server_alias: s for s in store.server_states()}
    assert states["broken"].paused and states["broken"].last_error == "AUTH_FAILED"
    assert not states["healthy"].paused and states["healthy"].last_error is None
    assert boundary.commands and all(host == "healthy.invalid" for host, _ in boundary.commands)
    assert boundary.attempts.count("broken.invalid") == 1
    assert boundary.trust.lookup("broken", "broken.invalid", 22) is None

    await Monitor(store, service, lambda: "v1").poll_once(now=10000)
    assert boundary.attempts.count("broken.invalid") == 1
    assert len(boundary.commands) == 2
    assert next(w for w in store.watches() if w.id == broken_watch.id) == broken_watch
    assert store.events() == [pending_event]

    # Correcting the profile changes its version and allows the same pending
    # watch to be polled again through the ordinary restart/retry mechanism.
    boundary.broken = False
    await Monitor(AgentStore(store.path), service, lambda: "v2").poll_once(now=10001)
    assert boundary.attempts.count("broken.invalid") > 1
    assert any(host == "broken.invalid" for host, _ in boundary.commands)
    assert all(not s.paused and s.last_error is None for s in store.server_states())
    assert all(w.active and w.last_state == "RUNNING" for w in store.watches(active=True))
    assert store.events() == [pending_event]


async def test_monitor_run_keeps_delivering_after_private_key_failure(tmp_path, boundary):
    store, service = monitor_store(tmp_path), query_service(tmp_path, boundary)
    stop, delivered, delays = asyncio.Event(), [], []
    pending_event = store.events()[0]

    async def after_poll():
        delivered.append(store.events())
        if len(delivered) == 2:
            stop.set()

    async def wait(delay):
        delays.append(delay)

    await Monitor(store, service, lambda: "v1", after_poll=after_poll,
                  wait=wait, jitter=lambda: 0).run(stop)
    assert delivered == [[pending_event], [pending_event]]
    assert delays == [45, 45]
    assert boundary.attempts.count("broken.invalid") == 1
    assert len(boundary.commands) == 2
    assert len(store.watches(active=True)) == 2


async def test_mcp_serializes_private_key_failure_without_sensitive_diagnostics(boundary, caplog):
    from fastmcp import Client
    from hpc_mcp.server import create_server

    async def execute(cmd_string, server_alias=None, directory=None, timeout_ms=None):
        return await boundary.client().run_general(cmd_string, directory, timeout_ms)

    container = SimpleNamespace(general_ssh=SimpleNamespace(execute=execute))
    async with Client(create_server(container)) as client:
        result = await client.call_tool("ssh_execute", {"cmd_string": "pwd", "directory": "/work"})
    assert result.data["ok"] is False
    assert result.data["error"]["code"] == "AUTH_FAILED"
    assert result.data["error"]["retriable"] is False
    assert result.data["error"]["details"] == {}
    for marker in (CONTENT_MARKER, PASSPHRASE_MARKER, KEY_MARKER, "KeyImportError"):
        assert marker not in json.dumps(result.data) + caplog.text


async def test_agent_cli_renders_private_key_failure_safely(boundary, capsys):
    import typer
    from hpc_mcp.agent.cli import _errors

    with pytest.raises(typer.Exit) as caught:
        with _errors():
            await operation(boundary.client(), "run")
    assert caught.value.exit_code == 1
    output = capsys.readouterr()
    assert "Agent error [AUTH_FAILED]" in output.err
    for marker in (CONTENT_MARKER, PASSPHRASE_MARKER, KEY_MARKER, "KeyImportError"):
        assert marker not in output.out + output.err
