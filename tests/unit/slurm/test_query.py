from pathlib import Path
from types import SimpleNamespace
from contextlib import asynccontextmanager
import posixpath
import stat

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry
from hpc_mcp.ssh.protocol import CommandResult


class FakeSshClient:
    def __init__(self, results: list[CommandResult] | None = None) -> None:
        self.results = list(results or [])
        self.commands = []

    async def run(self, command):
        self.commands.append(command)
        return self.results.pop(0)

    async def realpath(self, path):
        return posixpath.normpath(path)

    @asynccontextmanager
    async def sftp(self):
        yield self

    async def lstat(self, path):
        return SimpleNamespace(permissions=stat.S_IFDIR | 0o755)


class FakeRemoteFiles:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.size = 0
        self.entries: dict[str, list[SimpleNamespace]] = {}

    async def stat(self, project_id: str, relative_path: str):
        self.calls.append(("stat", project_id, relative_path))
        return SimpleNamespace(size=self.size)

    async def read(self, project_id: str, relative_path: str, offset: int = 0):
        self.calls.append(("read", project_id, relative_path, offset))
        length = min(256 * 1024, max(0, self.size - offset))
        return SimpleNamespace(
            content="x" * length,
            offset=offset,
            next_offset=None,
            size=self.size,
            truncated=False,
        )

    async def list(self, project_id: str, relative_path: str):
        self.calls.append(("list", project_id, relative_path))
        return self.entries.get(relative_path, [])


def query_service(
    tmp_path: Path,
    *,
    results: list[CommandResult] | None = None,
    artifact_directory: str = "output",
):
    from hpc_mcp.slurm.query import SlurmQueryService

    local_root = tmp_path / "project"
    local_root.mkdir()
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    project = registry.ensure(local_root, "cluster-a", "/srv/projects")
    job = registry.register_job(
        project.id,
        "123",
        "logs/123.out",
        "logs/123.err",
    )
    client = FakeSshClient(results)
    files = FakeRemoteFiles()
    service = SlurmQueryService(
        registry,
        client,
        username="alice",
        remote_files=files,
        artifact_directory=artifact_directory,
    )
    return service, registry, project, job, client, files


@pytest.mark.asyncio
async def test_squeue_uses_exact_structured_query_format(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(
        tmp_path,
        results=[CommandResult(exit_status=0, stdout="123|RUNNING|demo|gpu|00:01|node01\n", stderr="")],
    )

    jobs = await service.query_squeue("cluster-a", ["123"])

    command = client.commands[0]
    assert command.program == "squeue"
    assert command.args == (
        "--noheader", "--user", "alice", "--jobs", "123", "--format",
        "%i|%T|%j|%P|%M|%R",
    )
    assert jobs["123"].state == "RUNNING"
    assert jobs["123"].node_list == "node01"


@pytest.mark.asyncio
async def test_sacct_uses_exact_structured_query_format(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(
        tmp_path,
        results=[CommandResult(exit_status=0, stdout="123|COMPLETED|0:0|00:02|4|16G|node01\n", stderr="")],
    )

    jobs = await service.query_sacct("cluster-a", ["123"])

    command = client.commands[0]
    assert command.program == "sacct"
    assert command.args == (
        "--noheader", "--parsable2", "--user", "alice", "--jobs", "123",
        "--format", "JobID%64,State%64,ExitCode,Elapsed,AllocCPUS,ReqMem,NodeList",
    )
    assert jobs["123"].state == "COMPLETED"
    assert jobs["123"].exit_code == "0:0"


@pytest.mark.asyncio
async def test_accounting_requests_array_identity_instead_of_raw_numeric_id(tmp_path):
    service, registry, project, _, client, _ = query_service(tmp_path)
    registry.register_job(project.id, "123_7", None, None)
    async def run(command):
        client.commands.append(command)
        field = command.args[command.args.index("--format") + 1].split(",")[0]
        identity = "123_7" if field.startswith("JobID%") else "456"
        return CommandResult(exit_status=0, stdout=identity + "|COMPLETED|0:0|00:02|1|1G|n\n", stderr="")
    client.run = run
    jobs = await service.query_sacct("cluster-a", ["123_7"])
    assert jobs["123_7"].state == "COMPLETED"


@pytest.mark.asyncio
async def test_unregistered_job_is_rejected_before_ssh(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(tmp_path)

    with pytest.raises(HpcMcpError) as error:
        await service.get_job("999")

    assert error.value.code == "JOB_NOT_REGISTERED"
    assert client.commands == []


@pytest.mark.asyncio
async def test_query_rejects_a_mixed_unowned_batch_before_ssh(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(tmp_path)

    with pytest.raises(HpcMcpError) as error:
        await service.query_squeue("cluster-a", ["123", "999"])

    assert error.value.code == "JOB_NOT_REGISTERED"
    assert client.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize("job_id", ["123", "123_7"])
async def test_retired_controller_job_queries_only_main_accounting_row(tmp_path, job_id):
    service, registry, project, _, client, _ = query_service(
        tmp_path,
        results=[
            CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\n"),
            CommandResult(exit_status=0, stdout=(
                f"{job_id}|COMPLETED|0:0|00:02|4|16G|node01\n"
                f"{job_id}.batch|FAILED|1:0|00:02|4|16G|node01\n"
                "999|FAILED|2:0|00:02|4|16G|node01\n"
            ), stderr=""),
        ],
    )
    if job_id != "123":
        registry.register_job(project.id, job_id, None, None)

    job = await service.get_job(job_id)

    assert (job.job_id, job.state, job.exit_code) == (job_id, "COMPLETED", "0:0")
    assert [command.program for command in client.commands] == ["squeue", "sacct"]
    for command in client.commands:
        assert command.args[command.args.index("--jobs") + 1] == job_id
        assert command.args[command.args.index("--user") + 1] == "alice"


@pytest.mark.asyncio
async def test_retired_controller_empty_accounting_remains_unknown(tmp_path):
    service, _, _, _, client, _ = query_service(tmp_path, results=[
        CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\n"),
        CommandResult(exit_status=0, stdout="", stderr=""),
    ])
    job = await service.get_job("123")
    assert (job.state, job.exit_code) == ("UNKNOWN", None)
    assert len(client.commands) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [
    CommandResult(exit_status=1, stdout="", stderr="permission denied: synthetic-secret\n"),
    CommandResult(exit_status=1, stdout="", stderr="authentication failed: synthetic-secret\n"),
    CommandResult(exit_status=1, stdout="", stderr="Unable to contact slurm controller: synthetic-secret\n"),
    CommandResult(exit_status=255, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\n"),
    CommandResult(exit_status=1, stdout="123|RUNNING|demo|gpu|00:01|node01\n", stderr="slurm_load_jobs error: Invalid job id specified\n"),
    CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\n", truncated=True),
    CommandResult(exit_status=0, stdout="", stderr="", truncated=True),
    CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\nsynthetic-secret\n"),
    CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specif"),
])
async def test_squeue_failures_do_not_fall_back_to_accounting(tmp_path, result):
    service, _, _, _, client, _ = query_service(tmp_path, results=[result])
    with pytest.raises(HpcMcpError) as error:
        await service.get_job("123")
    assert error.value.code == "SLURM_QUERY_FAILED"
    assert "synthetic-secret" not in str(error.value)
    assert [command.program for command in client.commands] == ["squeue"]


@pytest.mark.asyncio
async def test_retired_controller_list_keeps_registered_batch_scope(tmp_path):
    service, registry, project, _, client, _ = query_service(tmp_path, results=[
        CommandResult(exit_status=1, stdout="", stderr="slurm_load_jobs error: Invalid job id specified\n"),
        CommandResult(exit_status=0, stdout="123|COMPLETED|0:0|00:02|4|16G|n\n123_7|FAILED|1:0|00:02|4|16G|n\n999|RUNNING|0:0|00:02|4|16G|n\n", stderr=""),
    ])
    registry.register_job(project.id, "123_7", None, None)
    jobs = await service.list_jobs()
    assert {(job.job_id, job.state) for job in jobs} == {("123", "COMPLETED"), ("123_7", "FAILED")}
    assert [command.program for command in client.commands] == ["squeue", "sacct"]
    for command in client.commands:
        assert set(command.args[command.args.index("--jobs") + 1].split(",")) == {"123", "123_7"}
        assert command.args[command.args.index("--user") + 1] == "alice"


@pytest.mark.asyncio
async def test_get_job_uses_sacct_only_when_job_is_absent_from_squeue(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(
        tmp_path,
        results=[
            CommandResult(exit_status=0, stdout="", stderr=""),
            CommandResult(exit_status=0, stdout="123|FAILED|1:0|00:02|4|16G|node01\n", stderr=""),
        ],
    )

    job = await service.get_job("123")

    assert [command.program for command in client.commands] == ["squeue", "sacct"]
    assert job.state == "FAILED"


@pytest.mark.asyncio
async def test_get_job_does_not_query_sacct_when_squeue_has_job(tmp_path: Path) -> None:
    service, _, _, _, client, _ = query_service(
        tmp_path,
        results=[CommandResult(exit_status=0, stdout="123|PENDING|demo|gpu|00:00|Priority\n", stderr="")],
    )

    job = await service.get_job("123")

    assert [command.program for command in client.commands] == ["squeue"]
    assert job.state == "PENDING"


@pytest.mark.asyncio
async def test_disappearance_from_both_queries_is_unknown_not_completed(tmp_path: Path) -> None:
    service, _, _, _, _, _ = query_service(
        tmp_path,
        results=[
            CommandResult(exit_status=0, stdout="", stderr=""),
            CommandResult(exit_status=0, stdout="", stderr=""),
        ],
    )

    job = await service.get_job("123")

    assert job.state == "UNKNOWN"


@pytest.mark.asyncio
async def test_list_jobs_batches_squeue_then_queries_only_missing_jobs_in_sacct(
    tmp_path: Path,
) -> None:
    service, registry, project, _, client, _ = query_service(
        tmp_path,
        results=[
            CommandResult(exit_status=0, stdout="123|RUNNING|demo|gpu|00:01|node01\n", stderr=""),
            CommandResult(exit_status=0, stdout="456|COMPLETED|0:0|00:02|4|16G|node02\n", stderr=""),
        ],
    )
    registry.register_job(project.id, "456", None, None)

    jobs = await service.list_jobs()

    # The registry deliberately resolves tied submission timestamps by UUID,
    # so this API has no insertion-order contract. Verify the returned job
    # states and batching semantics without depending on that random tie-break.
    assert {(job.job_id, job.state) for job in jobs} == {
        ("123", "RUNNING"),
        ("456", "COMPLETED"),
    }
    assert set(client.commands[0].args[4].split(",")) == {"123", "456"}
    assert client.commands[1].program == "sacct"
    assert client.commands[1].args[5] == "456"


@pytest.mark.asyncio
async def test_log_default_reads_final_256_kib_and_reports_truncation(tmp_path: Path) -> None:
    service, client = sftp_query_service(tmp_path)
    client.data = b"x" * (300 * 1024)

    chunk = await service.read_log("123", "stdout")

    expected_offset = len(client.data) - 256 * 1024
    assert client.seeks == [expected_offset]
    assert len(chunk.text) == 256 * 1024
    assert chunk.offset == expected_offset
    assert chunk.truncated is True


@pytest.mark.asyncio
async def test_artifacts_stay_under_configured_directory_and_cap_at_2000(tmp_path: Path) -> None:
    service, client = sftp_query_service(tmp_path, artifact_directory="results")
    client.directories[client.root + "/results"] = (
        f"artifact-{number:04d}.dat" for number in range(2001)
    )

    artifacts = await service.list_artifacts("123")

    assert client.scanned == [client.root + "/results"]
    assert len(artifacts) == 2000
    assert artifacts[0].relative_path == "artifact-0000.dat"
    assert all(not artifact.relative_path.startswith("results/") for artifact in artifacts)


@pytest.mark.asyncio
async def test_unsafe_artifact_directory_is_rejected_before_remote_access(tmp_path: Path) -> None:
    service, _, _, _, _, files = query_service(
        tmp_path, artifact_directory="../outside"
    )

    with pytest.raises(HpcMcpError) as error:
        await service.list_artifacts("123")

    assert error.value.code == "PATH_OUTSIDE_PROJECT"
    assert files.calls == []


class MemorySftp:
    """Lazy remote filesystem double; records actual byte and enumeration costs."""

    def __init__(self, root):
        self.root = root
        self.data = b""
        self.links = {}
        self.redirects = {}
        self.directories = {}
        self.read_requests = []
        self.seeks = []
        self.enumerated = 0
        self.scanned = []
        self.closed = False

    @asynccontextmanager
    async def sftp(self):
        yield self

    async def realpath(self, path):
        path = posixpath.normpath(path)
        for source, target in {**self.links, **self.redirects}.items():
            if path == source or path.startswith(source + "/"):
                return target + path[len(source):]
        return path

    async def lstat(self, path):
        mode = stat.S_IFDIR | 0o755
        if path in self.links:
            mode = stat.S_IFLNK | 0o777
        elif path.endswith((".out", ".err", ".dat")):
            mode = stat.S_IFREG | 0o644
        return SimpleNamespace(permissions=mode, size=len(self.data), mtime=1, mtime_ns=2)

    async def open(self, path, mode):
        assert mode == "rb"
        self.position = 0
        return self

    async def seek(self, offset):
        self.seeks.append(offset)
        self.position = offset

    async def read(self, size):
        self.read_requests.append(size)
        result = self.data[self.position:self.position + size]
        self.position += len(result)
        return result

    async def close(self):
        self.closed = True

    async def scandir(self, path):
        self.scanned.append(path)
        names = self.directories.get(path, ())
        if callable(names):
            names = names()
        for name in names:
            self.enumerated += 1
            yield SimpleNamespace(filename=name, attrs=await self.lstat(posixpath.join(path, name)))


def sftp_query_service(tmp_path, artifact_directory="output"):
    from hpc_mcp.slurm.query import SlurmQueryService
    _, registry, project, _, _, _ = query_service(tmp_path)
    client = MemorySftp(project.remote_root)
    service = SlurmQueryService(
        registry, client, username="alice", artifact_directory=artifact_directory,
    )
    return service, client


@pytest.mark.asyncio
async def test_log_reads_seek_and_never_transfer_more_than_256_kib(tmp_path):
    service, client = sftp_query_service(tmp_path)
    client.data = b"x" * (1024 * 1024)

    chunk = await service.read_log("123", "stdout")

    assert client.seeks == [len(client.data) - 256 * 1024]
    assert sum(client.read_requests) <= 256 * 1024
    assert len(chunk.text.encode()) == 256 * 1024
    assert chunk.offset == len(client.data) - 256 * 1024
    assert chunk.next_offset is None
    assert chunk.truncated
    assert client.closed


@pytest.mark.asyncio
async def test_multibyte_tail_advances_to_next_utf8_boundary(tmp_path):
    service, client = sftp_query_service(tmp_path)
    client.data = "界".encode() * 100_000
    raw_offset = len(client.data) - 256 * 1024

    chunk = await service.read_log("123", "stdout")

    assert chunk.offset == raw_offset + 1
    assert chunk.text == "界" * ((len(client.data) - chunk.offset) // 3)
    assert sum(client.read_requests) <= 256 * 1024
    assert chunk.next_offset is None
    assert chunk.truncated


@pytest.mark.asyncio
async def test_explicit_offset_has_bounded_utf8_pagination(tmp_path):
    service, client = sftp_query_service(tmp_path)
    client.data = "界".encode() * 100_000

    chunk = await service.read_log("123", "stdout", offset=0)

    assert chunk.offset == 0
    assert chunk.next_offset == 262143
    assert len(chunk.text.encode()) == 262143
    assert sum(client.read_requests) <= 256 * 1024


@pytest.mark.asyncio
@pytest.mark.parametrize("directory,link", [
    ("output", "output"),
    ("reports/output", "reports"),
    ("output", "output/subdir"),
])
async def test_artifact_root_ancestor_and_traversal_symlinks_are_rejected(tmp_path, directory, link):
    service, client = sftp_query_service(tmp_path, directory)
    client.links[posixpath.join(client.root, link)] = client.root + "/private"
    client.directories[client.root + "/output"] = ["subdir"]
    client.directories[client.root + "/private"] = ["secret.dat"]

    with pytest.raises(HpcMcpError) as error:
        await service.list_artifacts("123")

    assert error.value.code == "PATH_OUTSIDE_PROJECT"
    assert client.root + "/private" not in client.scanned


@pytest.mark.asyncio
async def test_canonical_child_must_remain_inside_artifact_root(tmp_path):
    service, client = sftp_query_service(tmp_path)
    client.directories[client.root + "/output"] = ["secret.dat"]
    client.redirects[client.root + "/output/secret.dat"] = client.root + "/private/secret.dat"

    with pytest.raises(HpcMcpError) as error:
        await service.list_artifacts("123")

    assert error.value.code == "PATH_OUTSIDE_PROJECT"


@pytest.mark.asyncio
async def test_large_directory_stops_enumerating_at_2000(tmp_path):
    service, client = sftp_query_service(tmp_path)

    def names():
        for number in range(100_000):
            assert number < 2000, "directory enumeration exceeded the global cap"
            yield f"artifact-{number:06}.dat"

    client.directories[client.root + "/output"] = names

    artifacts = await service.list_artifacts("123")

    assert len(artifacts) == 2000
    assert client.enumerated == 2000
    assert artifacts[0].relative_path == "artifact-000000.dat"


@pytest.mark.asyncio
async def test_artifact_cap_is_shared_by_nested_directories(tmp_path):
    service, client = sftp_query_service(tmp_path)
    client.directories[client.root + "/output"] = ["subdir"]
    client.directories[client.root + "/output/subdir"] = (
        f"artifact-{number:06}.dat" for number in range(3000)
    )

    artifacts = await service.list_artifacts("123")

    assert len(artifacts) == 2000
    assert client.enumerated == 2000
    assert artifacts[-1].relative_path == "subdir/artifact-001998.dat"
