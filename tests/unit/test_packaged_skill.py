"""Offline contract and distribution checks for the reusable workflow asset."""
import ast
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ASSET = Path("skills/hpc-slurm-workflow/SKILL.md")
RESOURCE = "hpc_mcp/" + ASSET.as_posix()
DESCRIPTION = (
    "Use when preparing, submitting, monitoring, collecting, or analyzing a Slurm "
    "job through the local hpc-mcp for any configured server or project."
)


def skill_text():
    assert (ROOT / ASSET).is_file(), "The generic workflow Skill must exist"
    return (ROOT / ASSET).read_text(encoding="utf-8")


def test_skill_frontmatter_and_portable_project_policy():
    text = skill_text()
    frontmatter = text.split("---", 2)[1].strip()
    assert frontmatter == f"name: hpc-slurm-workflow\ndescription: {DESCRIPTION}"
    assert len(frontmatter) < 1024
    assert "AGENTS.md" in text
    assert "mapping/output policy" in text
    assert "No project instructions" in text
    # An installed generic asset must not carry machine paths or SSH identities.
    assert not re.search(r"[A-Za-z]:[\\/]|/(?:home|Users|tmp|work)/|[\w.-]+@[\w.-]+", text)
    assert not re.search(r"server_alias\s*=\s*['\"]", text)


def test_submission_order_and_completion_safety_gates():
    text = skill_text()
    submission, completion = text.split("## Completion handoff", 1)
    submission = " ".join(submission.split())
    completion = " ".join(completion.split())
    ordered = [
        "server_test_connection(", "project_ensure(", "project_status(",
        "project_list_remote_changes(", "project_sync(", "slurm_preview(",
        "slurm_submit(", "hpc-agent", "slurm_watch_job(",
    ]
    offsets = [submission.index(token) for token in ordered]
    assert offsets == sorted(offsets)
    for gate in (
        "same UUID", "unchanged", "originating Codex terminal", "end the turn",
        "Do not poll", "Do not invent", "conflict", "ssh_execute", "remote_execute",
    ):
        assert gate in submission
    for gate in (
        "[hpc-event:<event-id>]", "quoted", "user claim", "hpc-agent status",
        "watch_id", "project_status(", "slurm_get_job(", "COMPLETED", "0:0",
        "slurm_read_log(", "slurm_list_artifacts(", "remote_list(",
        "remote_download(", "SHA-256", "current conversation",
        "OUT_OF_MEMORY", "PREEMPTED", "Do not automatically", "resubmit",
    ):
        assert gate in completion
    assert completion.index("slurm_get_job(") < completion.index("remote_download(")


def test_documented_tool_calls_match_registered_contracts():
    text = skill_text()
    contracts = {}
    for source in (ROOT / "src/hpc_mcp/tools").glob("*.py"):
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if isinstance(node, ast.AsyncFunctionDef) and any(
                isinstance(decorator, ast.Attribute) and decorator.attr == "tool"
                for decorator in node.decorator_list
            ):
                parameters = [arg.arg for arg in node.args.args]
                contracts[node.name] = (parameters, len(parameters) - len(node.args.defaults))
    calls = re.findall(r"`((?:server_|project_|slurm_|remote_)\w+\([^`]*\))`", text)
    assert len(calls) >= 14, "Keep callable signatures in the workflow"
    for example in calls:
        call = ast.parse(example, mode="eval").body
        assert isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
        assert call.func.id in contracts, example
        parameters, required_count = contracts[call.func.id]
        assert not call.keywords, "Examples use parameter names in positional order"
        assert required_count <= len(call.args) <= len(parameters), example
        assert [arg.id for arg in call.args] == parameters[:len(call.args)], example


def test_sdist_and_wheel_ship_the_installer_resource(tmp_path):
    uv = shutil.which("uv")
    assert uv, "Run distribution tests with uv available on PATH"
    distributions = tmp_path / "distributions"
    # Default uv build builds the wheel FROM the sdist, proving both routes.
    result = subprocess.run(
        [uv, "build", "--offline", "--out-dir", str(distributions), str(ROOT)],
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wheel, = distributions.glob("*.whl")
    sdist, = distributions.glob("*.tar.gz")
    with zipfile.ZipFile(wheel) as archive:
        assert RESOURCE in archive.namelist(), "Installer resource missing from wheel"
        packaged = archive.read(RESOURCE)
    assert packaged == (ROOT / ASSET).read_bytes()
    with tarfile.open(sdist) as archive:
        members = [member for member in archive if member.name.endswith("/" + ASSET.as_posix())]
        assert len(members) == 1
        assert archive.extractfile(members[0]).read() == packaged
    # Isolated import uses only the built wheel, never the editable source tree.
    result = subprocess.run(
        [sys.executable, "-I", "-c",
         "import sys; import importlib.resources as r; sys.path.insert(0, sys.argv[1]); "
         "sys.stdout.buffer.write(r.files('hpc_mcp').joinpath('skills/hpc-slurm-workflow/SKILL.md').read_bytes())",
         str(wheel)], capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert result.stdout == packaged


def test_sdist_excludes_private_docs_and_includes_public_helper(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    for name in ("src", "skills", "scripts", "docs"):
        shutil.copytree(ROOT / name, checkout / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for name in ("pyproject.toml", "uv.lock", "README.md", "AGENTS.md", "LICENSE", ".gitignore"):
        shutil.copyfile(ROOT / name, checkout / name)
    private = checkout / "docs/archive/private-provenance.md"
    private.parent.mkdir(parents=True)
    private.write_text("synthetic private provenance must not ship")
    result = subprocess.run([shutil.which("uv"), "build", "--offline", "--sdist", "--out-dir", str(tmp_path / "dist"), str(checkout)], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    with tarfile.open(next((tmp_path / "dist").glob("*.tar.gz"))) as archive:
        names = archive.getnames()
    assert any(name.endswith("/scripts/configure_windows.py") for name in names)
    assert any(name.endswith("/docs/install-with-codex.md") for name in names)
    assert not any("/docs/archive/" in name for name in names)
