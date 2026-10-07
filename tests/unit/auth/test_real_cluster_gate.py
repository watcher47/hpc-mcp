"""The opt-in boundary is checked locally, without SSH or a credential backend."""
import importlib.util
import json
from pathlib import Path

import pytest


def load_smoke_module():
    path = Path(__file__).parents[2] / "e2e" / "test_real_cluster.py"
    assert path.exists(), "The real-cluster test and its opt-in safety gate are missing"
    spec = importlib.util.spec_from_file_location("smoke_gate", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_real_cluster_disabled_skips_before_fixture_access(monkeypatch):
    monkeypatch.delenv("HPC_MCP_REAL_CLUSTER", raising=False)
    monkeypatch.setenv("HPC_MCP_REAL_CLUSTER_FIXTURE", "/does-not-exist.json")
    with pytest.raises(pytest.skip.Exception, match="HPC_MCP_REAL_CLUSTER=1"):
        load_smoke_module()._load_fixture()


def test_real_cluster_enabled_without_fixture_skips(monkeypatch):
    monkeypatch.setenv("HPC_MCP_REAL_CLUSTER", "1")
    monkeypatch.delenv("HPC_MCP_REAL_CLUSTER_FIXTURE", raising=False)
    with pytest.raises(pytest.skip.Exception, match="dedicated"):
        load_smoke_module()._load_fixture()


def test_real_cluster_rejects_undesignated_account(tmp_path, monkeypatch):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"dedicated_test_account": False}), "utf-8")
    monkeypatch.setenv("HPC_MCP_REAL_CLUSTER", "1")
    monkeypatch.setenv("HPC_MCP_REAL_CLUSTER_FIXTURE", str(fixture))
    with pytest.raises(pytest.fail.Exception, match="dedicated"):
        load_smoke_module()._load_fixture()


@pytest.mark.asyncio
async def test_smoke_workflow_uses_actual_mcp_serialization(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    import stat
    from types import SimpleNamespace
    from hpc_mcp.config.paths import AppPaths
    from hpc_mcp.project.registry import ProjectRegistry
    from hpc_mcp.project.sync import SyncResult
    from hpc_mcp.remote.files import ReadResult
    from hpc_mcp.slurm.query import SlurmJob, LogChunk
    from hpc_mcp.slurm.submit import SubmissionReceipt
    from hpc_mcp.ssh.protocol import CommandResult

    module = load_smoke_module()
    local_root = tmp_path / "fixture-project"
    local_root.mkdir()
    paths = AppPaths(tmp_path / "config", tmp_path / "state")
    paths.data_directory.mkdir()
    registry = ProjectRegistry(paths.data_directory / "projects.sqlite3")
    submits = []
    class Ssh:
        @asynccontextmanager
        async def sftp(self):
            async def lstat(path):
                return SimpleNamespace(permissions=stat.S_IFDIR | 0o700)
            yield SimpleNamespace(lstat=lstat)
        async def realpath(self, path):
            return "/home/fixture" if path == "." else path
        async def run(self, command):
            output = {"id": "fixture-user\n", "date": "2026-01-01T00:00:00\n",
                      "squeue": "99|RUNNING|100|node-a\n", "sacct": "123|\n"}[command.program]
            return CommandResult(exit_status=0, stdout=output, stderr="")
    async def ensure(local_root, server_alias):
        return registry.ensure(Path(local_root), server_alias, "/home/fixture/codex-projects")
    async def sync(project_id):
        return SyncResult(manifest_updated=True)
    async def read(**kwargs):
        return ReadResult("slurm/smoke.slurm", module.SMOKE_SCRIPT, 0, None, len(module.SMOKE_SCRIPT), "a" * 64, False)
    async def submit(project_id, script_path, request_id):
        submits.append(str(request_id))
        return SubmissionReceipt(str(request_id), "123", script_path, "2026-01-01T00:00:00+00:00")
    async def get_job(job_id):
        return SlurmJob(job_id, "COMPLETED")
    async def read_log(job_id, stream, offset):
        return LogChunk(job_id, "p", stream, "smoke-123.out", "HPC_MCP_SMOKE_OK\n", 0, None, 17, False)
    container = SimpleNamespace(registry=registry, ssh=lambda alias: Ssh(),
        projects=SimpleNamespace(ensure=ensure, sync=sync), remote_files=SimpleNamespace(read=read),
        slurm_submit=SimpleNamespace(submit=submit), slurm_query=SimpleNamespace(get_job=get_job, read_log=read_log))
    monkeypatch.setattr(module, "_load_fixture", lambda: ({"expected_username": "fixture-user"}, SimpleNamespace(alias="smoke-test", username="fixture-user"), local_root, paths))
    monkeypatch.setattr(module, "build_container", lambda paths, config_file=None: container)
    await module.test_real_cluster_read_sync_submit_and_log()
    assert len(submits) == 2 and submits[0] == submits[1]
    evidence = json.loads(next(paths.data_directory.glob("smoke-evidence-*.json")).read_text())
    assert evidence["completed"] and evidence["unrelated_jobs_unchanged"]
    assert evidence["account_job_ids"] == ["123"]


@pytest.mark.asyncio
async def test_smoke_rejects_existing_remote_symlink():
    from contextlib import asynccontextmanager
    import stat
    from types import SimpleNamespace
    module = load_smoke_module()
    assert hasattr(module, "_validate_remote_directory"), "remote fixture preflight must reject symlinks before ensure/sync"
    class Ssh:
        async def realpath(self, path):
            return path
        @asynccontextmanager
        async def sftp(self):
            async def lstat(path):
                return SimpleNamespace(permissions=stat.S_IFLNK | 0o777)
            yield SimpleNamespace(lstat=lstat)
    with pytest.raises(AssertionError, match="real directory"):
        await module._validate_remote_directory(Ssh(), "/home/test/codex-projects/fixture")
