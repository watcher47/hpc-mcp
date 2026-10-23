"""Local safety regressions; these do not claim real-cluster acceptance."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def workflow():
    path = Path(__file__).parents[1] / "scripts" / "cpu_smoke_workflow.py"
    assert path.exists(), "Reusable CPU workflow has not been implemented"
    spec = importlib.util.spec_from_file_location("cpu_smoke_workflow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_request_survives_restart_and_rejects_changed_intent(tmp_path):
    module = workflow()
    path = tmp_path / "run.json"
    first = module.load_or_create_run(path, {"script_sha256": "a"})
    assert module.load_or_create_run(path, {"script_sha256": "a"}) == first
    with pytest.raises(ValueError, match="intent"):
        module.load_or_create_run(path, {"script_sha256": "b"})
    assert json.loads(path.read_text())["request_id"] == first["request_id"]


def test_runtime_upgrade_does_not_change_workload_request_identity(tmp_path):
    module = workflow()
    path = tmp_path / "run.json"
    first = module.load_or_create_run(path, {"script_sha256": "a", "command": ["old-release"]})
    recovered = module.load_or_create_run(path, {"script_sha256": "a", "command": ["fixed-release"]})
    assert recovered["request_id"] == first["request_id"]
    assert recovered["intent"]["command"] == ["old-release"]


def test_unpack_handles_list_results_without_data():
    module = workflow()
    result = SimpleNamespace(data=None, content=[SimpleNamespace(type="text", text='[{"alias":"lab"}]')])
    assert module.unpack(result) == [{"alias": "lab"}]


@pytest.mark.parametrize("job", [
    {"job_id": "10.batch", "state": "COMPLETED", "exit_code": "0:0"},
    {"job_id": "10", "state": "COMPLETED", "exit_code": None},
    {"job_id": "10", "state": "FAILED", "exit_code": "1:0"},
])
def test_success_requires_exact_main_job_and_zero_exit(job):
    with pytest.raises(ValueError, match="accounting"):
        workflow().require_success(job, "10")


def test_completed_main_job_with_zero_exit_passes():
    workflow().require_success({"job_id": "10", "state": "COMPLETED", "exit_code": "0:0"}, "10")


async def test_existing_receipt_does_not_call_submit_again():
    module = workflow()
    instance = object.__new__(module.Workflow)
    instance.run = {"receipt": {"job_id": "10"}}
    # No client is present: any new preparation or MCP invocation would fail.
    await instance.submit()


def test_download_verifier_detects_changed_artifact(tmp_path):
    module = workflow()
    path = tmp_path / "result.json"
    path.write_bytes(b"correct")
    receipt = {"size": 7, "sha256": module.sha256(path)}
    module.verify_transfer(path, receipt)
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash"):
        module.verify_transfer(path, receipt)
