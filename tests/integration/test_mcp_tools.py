"""Exercise the actual MCP boundary without opening SSH connections."""
import json
import stat
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastmcp import Client

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.remote.patch import TextEdit


REQUEST = "00000000-0000-4000-8000-000000000001"
CASES = [
    ("slurm_watch_job", "slurm_watch", "verify", {"job_id": "123", "server_alias": "cluster"}),
    ("server_list", "general_ssh", "server_list", {}),
    ("ssh_execute", "general_ssh", "execute", {"cmd_string": "squeue --me", "server_alias": "cluster", "directory": "/work", "timeout_ms": 1000}),
    ("ssh_upload", "general_ssh", "upload", {"local_path": "/local/file", "remote_path": "/work/file", "server_alias": "cluster"}),
    ("ssh_download", "general_ssh", "download", {"remote_path": "/work/file", "local_path": "/local/file", "server_alias": "cluster"}),
    ("server_test_connection", "connection", "test", {"server_alias": "cluster"}),
    ("project_ensure", "projects", "ensure", {"local_root": "/tmp/project", "server_alias": "cluster"}),
    ("project_status", "projects", "status", {"project_id": "p"}),
    ("project_sync", "projects", "sync", {"project_id": "p"}),
    ("project_list_remote_changes", "projects", "list_remote_changes", {"project_id": "p"}),
    ("remote_list", "remote_files", "list", {"project_id": "p", "relative_path": "output"}),
    ("remote_stat", "remote_files", "stat", {"project_id": "p", "relative_path": "a.txt"}),
    ("remote_read", "remote_files", "read", {"project_id": "p", "relative_path": "a.txt", "offset": 2, "limit": 32}),
    ("remote_search", "remote_files", "search", {"project_id": "p", "pattern": "needle", "relative_path": ".", "max_matches": 12}),
    ("remote_apply_patch", "remote_patch", "apply", {"project_id": "p", "relative_path": "a.txt", "expected_sha256": "a" * 64, "edits": [{"old_text": "a", "new_text": "b", "expected_count": 1}]}),
    ("remote_upload", "remote_files", "upload", {"project_id": "p", "local_relative_path": "a.txt", "remote_relative_path": "b.txt"}),
    ("remote_download", "remote_files", "download", {"project_id": "p", "remote_relative_path": "a.txt", "local_relative_path": "b.txt"}),
    ("remote_execute", "execution", "execute", {"project_id": "p", "program": "pwd", "args": [], "relative_cwd": ".", "environment": {}, "timeout_seconds": 15}),
    ("slurm_preview", "slurm_preview", "preview", {"project_id": "p", "script_path": "job.sbatch"}),
    ("slurm_submit", "slurm_submit", "submit", {"project_id": "p", "script_path": "job.sbatch", "request_id": REQUEST}),
    ("slurm_get_job", "slurm_query", "get_job", {"job_id": "123"}),
    ("slurm_list_jobs", "slurm_query", "list_jobs", {}),
    ("slurm_read_log", "slurm_query", "read_log", {"job_id": "123", "stream": "stdout", "offset": 10}),
    ("slurm_list_artifacts", "slurm_query", "list_artifacts", {"job_id": "123"}),
]


@pytest.fixture
def fake_container():
    calls = []
    state = SimpleNamespace(calls=calls, error=None)
    for _, group, method, _ in CASES:
        if not hasattr(state, group):
            setattr(state, group, SimpleNamespace())

        def operation(group=group, method=method):
            async def call(**kwargs):
                calls.append((group, method, kwargs))
                if state.error is not None:
                    raise state.error
                return {"ok": True, "value": "service result"}
            return call

        setattr(getattr(state, group), method, operation())
    return state


@pytest.mark.asyncio
async def test_expected_tools_are_exposed(fake_container):
    from hpc_mcp.server import create_server

    async with Client(create_server(fake_container)) as client:
        listed = await client.list_tools()
    assert {tool.name for tool in listed} == {case[0] for case in CASES}
    for tool in listed:
        schemas = json.dumps([tool.input_schema, tool.output_schema]).lower()
        assert "password" not in schemas
        assert "credentials" not in schemas
        assert "container" not in schemas
    assert fake_container.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("name,group,method,arguments", CASES)
async def test_single_service_call_and_serialized_success(fake_container, name, group, method, arguments):
    from hpc_mcp.server import create_server

    async with Client(create_server(fake_container)) as client:
        result = await client.call_tool(name, arguments)
    assert result.data == {"ok": True, "value": "service result"}
    expected = dict(arguments)
    if name == "slurm_submit":
        expected["request_id"] = UUID(REQUEST)
    if name == "remote_apply_patch":
        expected["edits"] = [TextEdit("a", "b", 1)]
    assert fake_container.calls == [(group, method, expected)]


@pytest.mark.asyncio
@pytest.mark.parametrize("name,group,method,arguments", CASES)
async def test_domain_error_has_stable_tool_failure(fake_container, name, group, method, arguments):
    from hpc_mcp.server import create_server

    fake_container.error = HpcMcpError("REMOTE_CHANGED", "Remote file changed", True, {"revision": "new"})
    async with Client(create_server(fake_container)) as client:
        result = await client.call_tool(name, arguments)
    assert result.data == {"ok": False, "error": {"code": "REMOTE_CHANGED", "message": "Remote file changed", "retriable": True, "details": {"revision": "new"}}}
    assert len(fake_container.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("name,arguments", [
    ("slurm_submit", {"project_id": "p", "script_path": "job", "request_id": "invalid"}),
    ("remote_apply_patch", {"project_id": "p", "relative_path": "a", "expected_sha256": "a" * 64, "edits": [{"old_text": "", "new_text": "b"}]}),
    ("ssh_execute", {"cmd_string": "pwd", "timeout_ms": True}),
    ("ssh_execute", {"cmd_string": "pwd", "timeout_ms": "1000"}),
])
async def test_invalid_structured_inputs_do_not_call_service(fake_container, name, arguments):
    from hpc_mcp.server import create_server

    async with Client(create_server(fake_container)) as client:
        result = await client.call_tool(name, arguments, raise_on_error=False)
    assert result.is_error
    assert fake_container.calls == []


def test_main_selects_only_stdio(monkeypatch):
    from hpc_mcp import server

    sentinel = object()
    calls = []
    monkeypatch.setattr(server, "build_container", lambda config_file: sentinel)
    def create(container):
        assert container is sentinel
        return SimpleNamespace(run=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(server, "create_server", create)
    server.main([])
    assert calls == [{"transport": "stdio"}]


@pytest.fixture
def composed(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from hpc_mcp import container as module
    from hpc_mcp.config.paths import AppPaths
    from hpc_mcp.config.models import ClusterConfig
    from hpc_mcp.ssh.protocol import CommandResult

    calls = []
    realpath_overrides = {}
    class Sftp:
        async def makedirs(self, path, exist_ok):
            calls.append(("mkdir", path))
        async def lstat(self, path):
            return SimpleNamespace(permissions=stat.S_IFDIR | 0o755)
    class Ssh:
        def __init__(self, config, host_trust):
            self.alias = config.alias
        async def realpath(self, path):
            calls.append((self.alias, "realpath", path))
            if path in realpath_overrides:
                return realpath_overrides[path]
            return "/home/user" if path == "." else path
        @asynccontextmanager
        async def sftp(self):
            yield Sftp()
        async def run(self, command):
            calls.append((self.alias, "run", command))
            return CommandResult(exit_status=0, stdout="/usr/bin/tool\n", stderr="")
    monkeypatch.setattr(module, "SshClient", Ssh)
    paths = AppPaths(tmp_path / "config", tmp_path / "data")
    container = module.build_container(paths)
    assert calls == []
    for alias in ("one", "two"):
        container.configs.save_cluster(ClusterConfig(alias=alias, host="invalid.example", username="user", password="fixture-value"))
    container._test_realpath_overrides = realpath_overrides
    return container, calls, tmp_path


@pytest.mark.asyncio
async def test_composition_registers_absolute_roots_and_preserves_server_routing(composed):
    container, calls, root = composed
    project = await container.projects.ensure(str(root), "two")
    assert project.remote_root.startswith("/home/user/codex-projects/")
    assert await container.projects.status(project.id) == project
    await container.execution.execute(project.id, "pwd", [], ".", {}, 15)
    assert calls[-1][0:2] == ("two", "run")
    assert calls[-1][2].cwd == project.remote_root
    assert calls[-1][2].timeout_seconds == 15


@pytest.mark.asyncio
async def test_composed_missing_project_returns_stable_failure_without_ssh(composed):
    from hpc_mcp.server import create_server
    container, calls, _ = composed
    async with Client(create_server(container)) as client:
        result = await client.call_tool("remote_read", {"project_id": "missing", "relative_path": "x"})
    assert result.data["error"]["code"] == "PROJECT_NOT_FOUND"
    assert calls == []


@pytest.mark.asyncio
async def test_composed_execute_rejects_escape_and_blocked_command(composed):
    container, calls, root = composed
    project = await container.projects.ensure(str(root), "one")
    for program, cwd, code in [("pwd", "../", "PATH_OUTSIDE_PROJECT"), ("rm", ".", "COMMAND_BLOCKED")]:
        with pytest.raises(HpcMcpError) as caught:
            await container.execution.execute(project.id, program, [], cwd, {}, 15)
        assert caught.value.code == code
    assert not any(call[1] == "run" for call in calls)


@pytest.mark.asyncio
async def test_composed_execute_rejects_registered_root_replaced_by_symlink(composed):
    container, calls, root = composed
    project = await container.projects.ensure(str(root), "one")
    container._test_realpath_overrides[project.remote_root] = "/tmp/escape"

    with pytest.raises(HpcMcpError) as caught:
        await container.execution.execute(project.id, "pwd", [], ".", {}, 15)

    assert caught.value.code == "PATH_OUTSIDE_PROJECT"
    assert not any(call[1] == "run" for call in calls)


@pytest.mark.asyncio
async def test_composed_preview_maps_validation_error(composed, monkeypatch):
    from hpc_mcp import container as module
    container, _, root = composed
    project = await container.projects.ensure(str(root), "one")
    async def bad_preview(*args):
        raise ValueError("invalid script")
    monkeypatch.setattr(module, "preview_remote_submission", bad_preview)
    with pytest.raises(HpcMcpError) as caught:
        await container.slurm_preview.preview(project.id, "job")
    assert caught.value.code == "SLURM_SCRIPT_INVALID"


@pytest.mark.asyncio
async def test_composed_submit_registers_receipt_for_queries(composed, monkeypatch):
    from hpc_mcp import container as module
    from hpc_mcp.slurm.preview import create_submission_preview
    from hpc_mcp.slurm.submit import SubmissionReceipt
    container, _, root = composed
    project = await container.projects.ensure(str(root), "one")
    async def preview(*args):
        return create_submission_preview("job", "#!/bin/bash\n#SBATCH --output=output/%j.out\n#SBATCH --error=output/%j.err\necho hi\n")
    class Submit:
        def __init__(self, *args):
            pass
        async def submit(self, project_id, script_path, request_id):
            return SubmissionReceipt(str(request_id), "123", script_path, "2026-09-14T00:00:00Z")
    monkeypatch.setattr(module, "preview_remote_submission", preview)
    monkeypatch.setattr(module, "SlurmSubmitService", Submit)
    result = await container.slurm_submit.submit(project.id, "job", UUID(REQUEST))
    assert result.job_id == "123"
    assert result.request_id == REQUEST
    assert result.stdout_path == "output/123.out"
    assert result.stderr_path == "output/123.err"
    assert result.watch_command.startswith("hpc-agent ")
    assert "watch --server one --job-id 123 --project-id " + project.id in result.watch_command
    registered = container.registry.get_job("one", "123")
    assert registered.project_id == project.id
    assert registered.stdout_path == "output/123.out"
    assert registered.stderr_path == "output/123.err"


@pytest.mark.asyncio
@pytest.mark.parametrize("directive", [
    "#SBATCH --array=1-2",
    "#SBATCH -a 1-2",
    "#SBATCH --chdir=output",
    "#SBATCH -D output",
])
async def test_composed_submit_leaves_ambiguous_logs_unregistered(directive, composed, monkeypatch):
    """The MCP log tool must not follow a path guessed from array/chdir semantics."""
    from hpc_mcp import container as module
    from hpc_mcp.server import create_server
    from hpc_mcp.slurm.preview import create_submission_preview
    from hpc_mcp.slurm.submit import SubmissionReceipt

    container, calls, root = composed
    project = await container.projects.ensure(str(root), "one")

    class Submit:
        def __init__(self, *args):
            pass

        async def submit(self, project_id, script_path, request_id):
            return SubmissionReceipt(str(request_id), "123", script_path, "2026-09-14T00:00:00Z")

    async def preview(*args):
        return create_submission_preview(
            "job",
            f"#!/bin/bash\n{directive}\necho hi\n",
        )

    monkeypatch.setattr(module, "preview_remote_submission", preview)
    monkeypatch.setattr(module, "SlurmSubmitService", Submit)
    async with Client(create_server(container)) as client:
        receipt = await client.call_tool("slurm_submit", {
            "project_id": project.id,
            "script_path": "job",
            "request_id": REQUEST,
        })
        before_read = list(calls)
        log = await client.call_tool("slurm_read_log", {"job_id": receipt.data["job_id"]})

    registered = container.registry.get_job("one", "123")
    assert registered.stdout_path is None
    assert registered.stderr_path is None
    assert log.data["error"]["code"] == "LOG_NOT_REGISTERED"
    assert calls == before_read


@pytest.mark.asyncio
async def test_composed_preview_reads_through_routed_file_service(composed):
    from hpc_mcp.server import create_server
    from hpc_mcp.remote.files import ReadResult
    container, _, root = composed
    project = await container.projects.ensure(str(root), "two")
    calls = []
    class Files:
        def __init__(self, registry, client):
            assert client.alias == "two"
        async def read(self, project_id, relative_path, offset=0):
            calls.append((project_id, relative_path, offset))
            script = "#!/bin/bash\necho hi\n"
            return ReadResult(relative_path, script, 0, None, len(script), "a" * 64, False)
    container.remote_files._factory = Files
    async with Client(create_server(container)) as client:
        result = await client.call_tool("slurm_preview", {"project_id": project.id, "script_path": "job"})
    assert result.data["script_path"] == "job"
    assert calls == [(project.id, "job", 0)]


@pytest.mark.asyncio
async def test_service_dataclass_results_are_serialized(fake_container):
    from hpc_mcp.server import create_server
    from hpc_mcp.remote.files import ReadResult
    async def read(**kwargs):
        return ReadResult("a", "hello", 0, None, 5, "a" * 64, False)
    fake_container.remote_files.read = read
    async with Client(create_server(fake_container)) as client:
        result = await client.call_tool("remote_read", {"project_id": "p", "relative_path": "a"})
    assert result.data["content"] == "hello"
    assert result.data["next_offset"] is None


@pytest.mark.asyncio
async def test_composed_connection_uses_only_configured_server(composed):
    container, calls, _ = composed
    result = await container.connection.test("two")
    assert result["ready"] is True
    commands = [call for call in calls if call[1] == "run"]
    assert len(commands) == 3
    assert all(call[0] == "two" and call[2].program == "which" for call in commands)
    assert [call[2].args for call in commands] == [("sbatch",), ("squeue",), ("sacct",)]


@pytest.mark.asyncio
async def test_composed_sync_and_plan_use_project_server(composed):
    container, _, root = composed
    project = await container.projects.ensure(str(root), "two")
    calls = []
    class Sync:
        async def plan(self, record):
            calls.append(("plan", record))
            return {"remote_changes": ["a"]}
        async def execute(self, record):
            calls.append(("execute", record))
            return {"uploaded": ["b"]}
    def sync(alias):
        assert alias == "two"
        return Sync()
    container.projects._sync = sync
    assert await container.projects.list_remote_changes(project.id) == {"remote_changes": ["a"]}
    assert await container.projects.sync(project.id) == {"uploaded": ["b"]}
    assert calls == [("plan", project), ("execute", project)]
