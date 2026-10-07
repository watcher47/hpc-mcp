"""Opt-in safety gate. No configuration or SSH access before local approval checks."""
import asyncio
import json
import os
import stat
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

import pytest
from asyncssh import SFTPNoSuchFile
from fastmcp import Client

from hpc_mcp.config.paths import AppPaths, resolve_servers_path
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.container import build_container
from hpc_mcp.project.slug import project_slug
from hpc_mcp.server import create_server
from hpc_mcp.ssh.policy import RemoteCommand


pytestmark = pytest.mark.real_cluster
SMOKE_SCRIPT = """#!/bin/bash
#SBATCH --job-name=hpc-mcp-smoke
#SBATCH --time=00:01:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64M
#SBATCH --output=smoke-%j.out
#SBATCH --error=smoke-%j.err
set -eu
printf 'HPC_MCP_SMOKE_OK\\n'
"""


def _load_fixture():
    if os.environ.get("HPC_MCP_REAL_CLUSTER") != "1":
        pytest.skip("set HPC_MCP_REAL_CLUSTER=1 for the isolated test account")
    fixture_name = os.environ.get("HPC_MCP_REAL_CLUSTER_FIXTURE")
    if not fixture_name or not Path(fixture_name).is_file():
        pytest.skip("supply the dedicated account fixture with HPC_MCP_REAL_CLUSTER_FIXTURE")
    fixture_path = Path(fixture_name).resolve()
    try:
        fixture = json.loads(fixture_path.read_text("utf-8"))
        if not isinstance(fixture, dict) or fixture.get("dedicated_test_account") is not True:
            pytest.fail("fixture must explicitly identify a dedicated test account")
        if set(fixture) != {"dedicated_test_account", "server_alias",
                            "expected_username", "fixture_id"}:
            pytest.fail("fixture fields differ from the documented non-secret schema")
        fixture_id = str(UUID(fixture["fixture_id"]))
        alias = fixture["server_alias"]
        if not alias.startswith("smoke-"):
            pytest.fail("dedicated test alias must start with smoke-")
        user_paths = AppPaths.for_user()
        config = ConfigRepository(resolve_servers_path()).load_cluster(alias)
        if config.username != fixture["expected_username"]:
            pytest.fail("fixture account differs from the selected profile")
        if config.remote_projects_root != "~/codex-projects":
            pytest.fail("test profile must use ~/codex-projects")
        root = fixture_path.parent
        local_root = root / ("hpc-mcp-smoke-" + fixture_id)
        if not local_root.is_dir():
            pytest.fail("dedicated local fixture project is missing")
        entries = list(local_root.rglob("*"))
        if any(path.is_symlink() for path in entries) or local_root.is_symlink():
            pytest.fail("local fixture project must contain no symlinks")
        files = {path.relative_to(local_root).as_posix() for path in entries if path.is_file()}
        if files != {"slurm/smoke.slurm"}:
            pytest.fail("local fixture must contain only slurm/smoke.slurm")
        if (local_root / "slurm/smoke.slurm").read_text("utf-8") != SMOKE_SCRIPT:
            pytest.fail("smoke script must match the fixed one-minute CPU-only script")
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pytest.fail("invalid dedicated fixture; follow the README schema")
    return fixture, config, local_root, AppPaths(user_paths.config_directory, root / ".test-state")


async def _run(ssh, home, program, args):
    result = await ssh.run(RemoteCommand(program, args, cwd=home))
    assert result.exit_status == 0 and not result.truncated, "scheduler audit command failed"
    return result.stdout


async def _queue(ssh, home):
    raw = await _run(ssh, home, "squeue", ["--noheader", "--format", "%i|%T|%p|%N"])
    rows = {}
    for line in raw.splitlines():
        fields = line.strip().split("|")
        assert len(fields) == 4 and fields[0] not in rows, "invalid queue audit output"
        rows[fields[0]] = fields[1:]
    return rows


async def _call(client, name, **arguments):
    result = await client.call_tool(name, arguments)
    assert not result.is_error, name
    assert not isinstance(result.data, dict) or result.data.get("ok") is not False, name
    return result.data


async def _validate_remote_directory(ssh, expected_root):
    parent = str(PurePosixPath(expected_root).parent)
    assert await ssh.realpath(parent) == parent, "codex-projects must be a real directory"
    async with ssh.sftp() as sftp:
        try:
            attrs = await sftp.lstat(expected_root)
        except (FileNotFoundError, SFTPNoSuchFile):
            return
        assert stat.S_ISDIR(attrs.permissions or 0), "remote fixture must be a real directory"
        assert await ssh.realpath(expected_root) == expected_root, "remote fixture resolves outside its dedicated root"


@pytest.mark.asyncio
async def test_real_cluster_read_sync_submit_and_log():
    fixture, config, local_root, paths = _load_fixture()
    container = build_container(paths, config_file=resolve_servers_path())
    ssh = container.ssh(config.alias)
    home = await ssh.realpath(".")
    actual_user = await _run(ssh, home, "id", ["-un"])
    assert actual_user.strip() == fixture["expected_username"], "remote account mismatch"
    expected_root = str(PurePosixPath(home) / "codex-projects" / project_slug(local_root))
    # Validate persisted mapping before project_ensure can create a remote directory.
    mapped = container.registry.ensure(local_root, config.alias, str(PurePosixPath(home) / "codex-projects"))
    assert mapped.remote_root == expected_root, "persisted fixture mapping is outside its dedicated root"
    await _validate_remote_directory(ssh, expected_root)
    before = await _queue(ssh, home)
    started = (await _run(ssh, home, "date", ["+%Y-%m-%dT%H:%M:%S"])).strip()
    request_id = str(uuid4())
    evidence = {"started_at": datetime.now(timezone.utc).isoformat(), "request_id": request_id,
                "before": before, "expected_remote_root": expected_root}
    evidence_path = paths.data_directory / ("smoke-evidence-" + request_id + ".json")
    job_id = None
    try:
        async with Client(create_server(container)) as client:
            project = await _call(client, "project_ensure", local_root=str(local_root), server_alias=config.alias)
            repeated = await _call(client, "project_ensure", local_root=str(local_root), server_alias=config.alias)
            assert project["id"] == repeated["id"] and project["remote_root"] == repeated["remote_root"] == expected_root
            sync = await _call(client, "project_sync", project_id=project["id"])
            assert sync["conflicts"] == [] and sync["failed"] == []
            script = await _call(client, "remote_read", project_id=project["id"], relative_path="slurm/smoke.slurm")
            assert script["content"] == SMOKE_SCRIPT
            arguments = {"project_id": project["id"], "script_path": "slurm/smoke.slurm", "request_id": request_id}
            receipt = await _call(client, "slurm_submit", **arguments)
            job_id = receipt["job_id"]
            evidence["job_id"] = job_id
            assert (await _call(client, "slurm_submit", **arguments))["job_id"] == job_id
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                final = await _call(client, "slurm_get_job", job_id=job_id)
                if final["state"] in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED"}:
                    break
                await asyncio.sleep(2)
            assert final["state"] == "COMPLETED", "smoke job did not complete within 180 seconds; retain its receipt"
            log = await _call(client, "slurm_read_log", job_id=job_id, stream="stdout")
            assert "HPC_MCP_SMOKE_OK" in log["text"]
            # Accounting excludes steps, and the dedicated account must remain idle otherwise.
            accounting = await _run(ssh, home, "sacct", ["--noheader", "--parsable2", "--allocations", "--user", config.username, "--starttime", started, "--format", "JobIDRaw"])
            created_ids = {row.strip().rstrip("|") for row in accounting.splitlines() if row.strip()}
            evidence["account_job_ids"] = sorted(created_ids)
            assert created_ids == {job_id}, "expected exactly one allocation for the dedicated test account"
            evidence["completed"] = True
    finally:
        try:
            after = await _queue(ssh, home)
            evidence["after"] = after
            unrelated_before = {key: value for key, value in before.items() if key != job_id}
            unrelated_after = {key: value for key, value in after.items() if key != job_id}
            evidence["unrelated_jobs_unchanged"] = unrelated_before == unrelated_after
        finally:
            evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", "utf-8")
        assert evidence["unrelated_jobs_unchanged"], "unrelated queue changed: gate is inconclusive; inspect saved evidence"
