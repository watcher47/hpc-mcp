import hashlib
import os
from pathlib import Path

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry


def test_same_local_project_reuses_remote_directory(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    local_root = tmp_path / "Research Project"
    local_root.mkdir()

    first = registry.ensure(local_root, "default", "~/codex-projects")
    second = registry.ensure(local_root, "default", "~/codex-projects")

    assert first.id == second.id
    assert first.remote_root == second.remote_root
    assert first.remote_root.endswith("/research-project")


def test_mapping_persists_when_registry_is_reopened(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    local_root = tmp_path / "Persistent Project"
    local_root.mkdir()

    first = ProjectRegistry(state_path).ensure(
        local_root, "default", "~/codex-projects"
    )
    second = ProjectRegistry(state_path).ensure(
        local_root, "default", "/different/base"
    )

    assert second == first


def test_same_slug_on_one_server_gets_path_derived_suffix(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    first_root = tmp_path / "first" / "Shared Name"
    second_root = tmp_path / "second" / "Shared Name"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)

    first = registry.ensure(first_root, "default", "~/codex-projects")
    second = registry.ensure(second_root, "default", "~/codex-projects")
    normalized_path = os.path.normcase(str(second_root.resolve()))
    expected_hash = hashlib.sha256(normalized_path.encode("utf-8")).hexdigest()[:8]

    assert first.remote_root == "~/codex-projects/shared-name"
    assert second.remote_root == f"~/codex-projects/shared-name-{expected_hash}"


def test_same_remote_root_can_be_used_on_different_servers(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    local_root = tmp_path / "Research Project"
    local_root.mkdir()

    first = registry.ensure(local_root, "cluster-a", "~/codex-projects")
    second = registry.ensure(local_root, "cluster-b", "~/codex-projects")

    assert first.id != second.id
    assert first.remote_root == second.remote_root


def test_register_job_is_additive_and_persists_when_reopened(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    local_root = tmp_path / "Project"
    local_root.mkdir()
    registry = ProjectRegistry(state_path)
    project = registry.ensure(local_root, "cluster-a", "/srv/projects")

    first = registry.register_job(project.id, "123", "logs/a.out", "logs/a.err")
    reopened = ProjectRegistry(state_path)
    second = reopened.register_job(project.id, "123", "changed.out", "changed.err")

    assert second == first
    assert reopened.get(project.id) == project
    assert reopened.get_job("cluster-a", "123") == first
    assert reopened.list_jobs() == [first]


def test_register_job_never_reassigns_existing_job_to_another_project(
    tmp_path: Path,
) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first_project = registry.ensure(first_root, "cluster-a", "/srv/projects")
    second_project = registry.ensure(second_root, "cluster-a", "/srv/projects")
    original = registry.register_job(first_project.id, "123", None, None)

    with pytest.raises(HpcMcpError) as error:
        registry.register_job(second_project.id, "123", None, None)

    assert error.value.code == "JOB_ALREADY_REGISTERED"
    assert registry.get_job("cluster-a", "123") == original


def test_job_id_is_unique_per_server_but_reusable_on_another_server(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first_project = registry.ensure(first_root, "cluster-a", "/srv/projects")
    second_project = registry.ensure(second_root, "cluster-b", "/srv/projects")

    first = registry.register_job(first_project.id, "123", None, None)
    second = registry.register_job(second_project.id, "123", None, None)

    assert first.id != second.id
    # `list_jobs` sorts equal submission timestamps by random record UUID;
    # uniqueness per server is the contract under test, not insertion order.
    assert {(record.server_alias, record.job_id) for record in registry.list_jobs()} == {
        ("cluster-a", "123"),
        ("cluster-b", "123"),
    }


def test_register_job_rejects_unknown_project(tmp_path: Path) -> None:
    registry = ProjectRegistry(tmp_path / "state.sqlite3")

    with pytest.raises(HpcMcpError) as error:
        registry.register_job("missing", "123", None, None)

    assert error.value.code == "PROJECT_NOT_FOUND"


@pytest.mark.parametrize("job_id", ["123,999", "123\n999", "", "123.batch"])
def test_register_job_rejects_unsafe_job_id(tmp_path: Path, job_id: str) -> None:
    local_root = tmp_path / "Project"
    local_root.mkdir()
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    project = registry.ensure(local_root, "cluster-a", "/srv/projects")

    with pytest.raises(HpcMcpError) as error:
        registry.register_job(project.id, job_id, None, None)

    assert error.value.code == "INVALID_JOB_ID"
