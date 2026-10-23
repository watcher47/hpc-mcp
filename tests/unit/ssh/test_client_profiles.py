import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import asyncssh
import pytest

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.client import SshClient
from hpc_mcp.ssh.policy import RemoteCommand
from hpc_mcp.ssh.trust import HostTrustStore


class FakeKey:
    def __init__(self, fingerprint: str = "SHA256:fixture") -> None:
        self.fingerprint = fingerprint

    def get_fingerprint(self, hash_name: str = "sha256") -> str:
        assert hash_name == "sha256"
        return self.fingerprint


class BytesStream:
    def __init__(self, content: bytes = b"") -> None:
        self.content = content

    async def read(self, max_bytes: int) -> bytes:
        chunk = self.content[:max_bytes]
        self.content = self.content[max_bytes:]
        return chunk


class FinishedProcess:
    def __init__(self, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.exit_status = 0
        self.stdout = BytesStream(stdout)
        self.stderr = BytesStream(stderr)

    async def wait_closed(self) -> None:
        return None

    def close(self) -> None:
        pass


def _config(**updates: object) -> ClusterConfig:
    values: dict[str, object] = {
        "alias": "default",
        "host": "compute.invalid",
        "username": "fixture-user",
        "password": "fixture-value",
        "connectionTimeoutMs": 16000,
        "keepaliveIntervalMs": 11000,
        "keepaliveCountMax": 4,
    }
    values.update(updates)
    return ClusterConfig.model_validate(values)


async def test_gpu_probe_stdin_is_sent_as_bytes(tmp_path, monkeypatch):
    from hpc_mcp.slurm import resource_probe
    source = Path(resource_probe.__file__).read_bytes()
    calls = []
    class Connection:
        async def create_process(self, command, **options):
            calls.append(options)
            return FinishedProcess(b'{}')
    @asynccontextmanager
    async def connect(): yield Connection()
    client = SshClient(_config(), HostTrustStore(tmp_path / 'trust.sqlite3'))
    monkeypatch.setattr(client, '_connect', connect)
    await client.run(RemoteCommand('python3', ['-I', '-', 'inventory'], '/project', standard_input=source))
    assert calls == [{'encoding':None, 'input':source}]


@pytest.mark.asyncio
async def test_password_profile_connects_with_tofu_and_records_after_authentication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: dict[str, object] = {}
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def create_process(
            self, command: str, **options: object
        ) -> FinishedProcess:
            recorded["command"] = command
            recorded["process_options"] = options
            return FinishedProcess()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        recorded.update(host=host, **options)
        verifier = options["client_factory"]()
        assert verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield Connection()

    monkeypatch.setattr(asyncssh, "connect", connect)

    await SshClient(_config(), trust).run(RemoteCommand("pwd", [], "/work"))

    assert recorded["password"] == "fixture-value"
    assert recorded["known_hosts"] == b""
    assert recorded["connect_timeout"] == 16.0
    assert recorded["keepalive_interval"] == 11.0
    assert recorded["keepalive_count_max"] == 4
    assert recorded["command"] == "cd -- /work && pwd"
    assert recorded["process_options"] == {
        "encoding": None,
        "stdin": asyncssh.DEVNULL,
    }
    assert trust.lookup("default", "compute.invalid", 22) == "SHA256:fixture"


@pytest.mark.asyncio
async def test_authentication_failure_never_records_first_host_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        assert verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        raise asyncssh.PermissionDenied("fixture-denial")
        yield  # pragma: no cover

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        await SshClient(_config(), trust).run(RemoteCommand("pwd", [], "/work"))

    assert error.value.code == "AUTH_FAILED"
    assert trust.lookup("default", "compute.invalid", 22) is None


@pytest.mark.asyncio
async def test_private_key_profile_uses_key_without_password(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded: dict[str, object] = {}
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def create_process(
            self, command: str, **options: object
        ) -> FinishedProcess:
            return FinishedProcess()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        recorded.update(options)
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield Connection()

    monkeypatch.setattr(asyncssh, "connect", connect)
    config = _config(
        password=None, privateKey="fixture-key-path", passphrase="fixture-phrase"
    )

    await SshClient(config, trust).run(RemoteCommand("pwd", [], "/work"))

    assert recorded["password"] is None
    assert recorded["client_keys"] == ["fixture-key-path"]
    assert recorded["passphrase"] == "fixture-phrase"
    assert recorded["public_key_auth"] is True


@pytest.mark.asyncio
async def test_sftp_start_is_bounded_by_profile_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def start_sftp_client(self) -> object:
            await asyncio.sleep(0.05)
            return object()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield Connection()

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        async with SshClient(_config(sftpTimeoutMs=1), trust).sftp():
            pass

    assert error.value.code == "SSH_TIMEOUT"


@pytest.mark.asyncio
async def test_command_policy_rejects_before_any_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    called = False

    def connect(*args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("connection must not be attempted")

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        await SshClient(_config(), HostTrustStore(tmp_path / "trust.sqlite3")).run(
            RemoteCommand("rm", ["result"], "/work")
        )

    assert error.value.code == "COMMAND_BLOCKED"
    assert called is False


@pytest.mark.asyncio
async def test_profile_output_limit_truncates_both_streams(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def create_process(
            self, command: str, **options: object
        ) -> FinishedProcess:
            return FinishedProcess(b"abcdef", b"uvwxyz")

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield Connection()

    monkeypatch.setattr(asyncssh, "connect", connect)

    result = await SshClient(_config(maxOutputBytes=4), trust).run(
        RemoteCommand("pwd", [], "/work")
    )

    assert result.stdout == "abcd"
    assert result.stderr == "uvwx"
    assert result.truncated is True


@pytest.mark.asyncio
async def test_sftp_and_realpath_use_authenticated_tofu_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")
    closed = False

    class Sftp:
        async def realpath(self, path: str) -> str:
            return "/home/fixture/project"

    class Connection:
        async def start_sftp_client(self) -> Sftp:
            return Sftp()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        nonlocal closed
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        try:
            yield Connection()
        finally:
            closed = True

    monkeypatch.setattr(asyncssh, "connect", connect)

    result = await SshClient(_config(), trust).realpath("~/project")

    assert result == "/home/fixture/project"
    assert closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "code", "retriable"),
    [
        (asyncssh.PermissionDenied("fixture-detail"), "AUTH_FAILED", False),
        (
            asyncssh.HostKeyNotVerifiable("fixture-detail"),
            "HOST_KEY_VERIFICATION_FAILED",
            False,
        ),
        (TimeoutError("fixture-detail"), "SSH_TIMEOUT", True),
        (OSError("fixture-detail"), "SSH_TRANSPORT_FAILED", True),
    ],
)
async def test_connection_failures_map_to_safe_stable_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    code: str,
    retriable: bool,
) -> None:
    def connect(*args: object, **kwargs: object) -> object:
        raise failure

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        await SshClient(_config(), HostTrustStore(tmp_path / "trust.sqlite3")).run(
            RemoteCommand("pwd", [], "/work")
        )

    assert error.value.code == code
    assert error.value.retriable is retriable
    assert error.value.details == {}
    assert "fixture-detail" not in error.value.message
    assert "fixture-value" not in error.value.message
    assert error.value.__cause__ is None
    assert error.value.__context__ is None


@pytest.mark.asyncio
async def test_sftp_preserves_caller_exception_when_teardown_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def start_sftp_client(self) -> object:
            return object()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        try:
            yield Connection()
        finally:
            raise OSError("fixture-teardown-detail")

    monkeypatch.setattr(asyncssh, "connect", connect)
    caller_error = ValueError("caller-owned")

    with pytest.raises(ValueError) as error:
        async with SshClient(_config(), trust).sftp():
            raise caller_error

    assert error.value is caller_error
    assert error.value.__cause__ is None


@pytest.mark.asyncio
async def test_sftp_maps_teardown_only_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Connection:
        async def start_sftp_client(self) -> object:
            return object()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        try:
            yield Connection()
        finally:
            raise OSError("fixture-teardown-detail")

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        async with SshClient(_config(), trust).sftp():
            pass

    assert error.value.code == "SSH_TRANSPORT_FAILED"
    assert error.value.__cause__ is None
    assert error.value.__context__ is None


@pytest.mark.asyncio
async def test_realpath_maps_owned_operation_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trust = HostTrustStore(tmp_path / "trust.sqlite3")

    class Sftp:
        async def realpath(self, path: str) -> str:
            raise OSError("fixture-remote-detail")

    class Connection:
        async def start_sftp_client(self) -> Sftp:
            return Sftp()

    @asynccontextmanager
    async def connect(host: str, **options: object):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield Connection()

    monkeypatch.setattr(asyncssh, "connect", connect)

    with pytest.raises(HpcMcpError) as error:
        await SshClient(_config(), trust).realpath("~/project")

    assert error.value.code == "SSH_TRANSPORT_FAILED"
    assert "fixture-remote-detail" not in error.value.message
    assert error.value.__cause__ is None


def _install_connection(monkeypatch, connection):
    @asynccontextmanager
    async def connect(host, **options):
        verifier = options["client_factory"]()
        verifier.validate_host_public_key(host, "192.0.2.20", 22, FakeKey())
        yield connection
    monkeypatch.setattr(asyncssh, "connect", connect)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["create", "drain", "wait"])
@pytest.mark.parametrize("profile_ms,call_seconds", [(1, 1), (1000, 0.001)])
async def test_command_lifecycle_uses_stricter_timeout(
    tmp_path, monkeypatch, phase, profile_ms, call_seconds
):
    class SlowStream(BytesStream):
        async def read(self, max_bytes):
            await asyncio.sleep(0.04)
            return b""

    class Process(FinishedProcess):
        async def wait_closed(self):
            if phase == "wait":
                await asyncio.sleep(0.04)

    class Connection:
        async def create_process(self, *args, **kwargs):
            if phase == "create":
                await asyncio.sleep(0.04)
            process = Process()
            if phase == "drain":
                process.stdout = SlowStream()
            return process

    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(commandTimeoutMs=profile_ms), HostTrustStore(tmp_path / "trust.db"))
    with pytest.raises(HpcMcpError) as error:
        await client.run(RemoteCommand("pwd", [], "/work", timeout_seconds=call_seconds))
    assert error.value.code == "SSH_TIMEOUT"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["realpath", "put", "get"])
async def test_sftp_timeout_bounds_actual_operations(tmp_path, monkeypatch, operation):
    cancelled = False

    class Sftp:
        async def slow(self, *args):
            nonlocal cancelled
            try:
                await asyncio.sleep(0.04)
                return "/work"
            except asyncio.CancelledError:
                cancelled = True
                raise
        realpath = put = get = slow

    class Connection:
        async def start_sftp_client(self):
            return Sftp()

    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(sftpTimeoutMs=1), HostTrustStore(tmp_path / "trust.db"))
    with pytest.raises(HpcMcpError) as error:
        if operation == "realpath":
            await client.realpath("/work")
        else:
            async with client.sftp() as sftp:
                await getattr(sftp, operation)("source", "destination")
    assert error.value.code == "SSH_TIMEOUT"
    assert cancelled


@pytest.mark.asyncio
async def test_large_streams_drain_concurrently_with_bounded_capture(tmp_path, monkeypatch):
    started = [asyncio.Event(), asyncio.Event()]
    exhausted = [asyncio.Event(), asyncio.Event()]

    class CoordinatedStream:
        def __init__(self, index):
            self.index = index
            self.chunks = 16

        async def read(self, size):
            assert size <= 64 * 1024
            started[self.index].set()
            # Sequential draining deadlocks here until the command timeout.
            await started[1 - self.index].wait()
            if self.chunks:
                self.chunks -= 1
                return bytes([65 + self.index]) * size
            exhausted[self.index].set()
            return b""

    class Process:
        stdout = CoordinatedStream(0)
        stderr = CoordinatedStream(1)
        exit_status = 0

        async def wait_closed(self):
            await asyncio.gather(*(event.wait() for event in exhausted))

    class Connection:
        async def create_process(self, *args, **kwargs):
            return Process()

    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(maxOutputBytes=1024), HostTrustStore(tmp_path / "trust.db"))
    result = await client.run(RemoteCommand("pwd", [], "/work", timeout_seconds=1))
    assert result.stdout == "A" * 1024
    assert result.stderr == "B" * 1024
    assert result.truncated
    assert all(event.is_set() for event in exhausted)


@pytest.mark.asyncio
@pytest.mark.parametrize("stdout,stderr", [(b"abcd", b""), (b"ab", b"cd"), (b"abcdef", b"")])
async def test_general_execution_stops_at_combined_output_limit(tmp_path, monkeypatch, stdout, stderr):
    class Process(FinishedProcess):
        closed = False
        def close(self):
            self.closed = True
    process = Process(stdout, stderr)
    class Connection:
        async def create_process(self, *args, **kwargs):
            return process
    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(whitelist=["pwd"], maxOutputBytes=4), HostTrustStore(tmp_path / "trust.db"))
    with pytest.raises(HpcMcpError) as error:
        await client.run_general("pwd", "/work", None)
    assert error.value.code == "OUTPUT_LIMIT_EXCEEDED"
    assert process.closed


@pytest.mark.asyncio
async def test_general_execution_revalidates_policy_before_connect(tmp_path, monkeypatch):
    def connect(*args, **kwargs):
        pytest.fail("blocked command reached SSH")
    monkeypatch.setattr(asyncssh, "connect", connect)
    client = SshClient(_config(), HostTrustStore(tmp_path / "trust.db"))
    with pytest.raises(HpcMcpError) as error:
        await client.run_general("pwd", "/work", None)
    assert error.value.code == "COMMAND_BLOCKED"


@pytest.mark.asyncio
async def test_general_execution_redacts_credentials_in_command_output(tmp_path, monkeypatch):
    class Connection:
        async def create_process(self, *args, **kwargs):
            return FinishedProcess(b"fixture-value", b"fixture-phrase")
    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(whitelist=["pwd"], passphrase="fixture-phrase"), HostTrustStore(tmp_path / "trust.db"))
    result = await client.run_general("pwd", "/work", None)
    assert result.stdout == "[REDACTED]"
    assert result.stderr == "[REDACTED]"


@pytest.mark.asyncio
async def test_general_execution_timeout_closes_process(tmp_path, monkeypatch):
    class SlowProcess(FinishedProcess):
        closed = False
        def close(self):
            self.closed = True
        async def wait_closed(self):
            await asyncio.sleep(0.1)
    process = SlowProcess()
    class Connection:
        async def create_process(self, *args, **kwargs):
            return process
    _install_connection(monkeypatch, Connection())
    client = SshClient(_config(whitelist=["pwd"], commandTimeoutMs=1), HostTrustStore(tmp_path / "trust.db"))
    with pytest.raises(HpcMcpError) as error:
        await client.run_general("pwd", "/work", 5000)
    assert error.value.code == "SSH_TIMEOUT"
    assert process.closed
