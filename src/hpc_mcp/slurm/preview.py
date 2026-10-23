from __future__ import annotations

import re
from typing import Any

from hpc_mcp.slurm.models import SlurmResources, SubmissionPreview
from hpc_mcp.slurm.parser import parse_sbatch_directives


MAX_SCRIPT_BYTES = 1024 * 1024
_MEMORY = re.compile(r"^(?P<count>[1-9]\d*)(?P<unit>[KMGTP](?:i?B)?|B)?$", re.IGNORECASE)
# Slurm time limits: minutes; hours:minutes[:seconds]; or days-hours[:minutes[:seconds]].
_TIME = re.compile(
    r"^(?:"
    r"[1-9]\d*"
    r"|(?:0\d?|[1-9]\d*):[0-5]\d(?::[0-5]\d)?"
    r"|[1-9]\d*-(?:[01]?\d|2[0-3])(?::[0-5]\d(?::[0-5]\d)?)?"
    r")$"
)


def create_submission_preview(script_path: str, script: str) -> SubmissionPreview:
    """Validate a batch script and return data only; it never submits a job."""
    _validate_path(script_path)
    if len(script.encode("utf-8")) > MAX_SCRIPT_BYTES:
        raise ValueError("Slurm script exceeds the 1 MiB preview limit")
    if not script.startswith("#!"):
        raise ValueError("Slurm script must start with a shebang")
    resources = parse_sbatch_directives(script)
    _validate_resources(resources)
    return SubmissionPreview(
        script_path=script_path,
        resources=resources,
        entry_command=resources.entry_command,
        warnings=_warnings(resources),
    )


async def preview_remote_submission(
    remote_files: Any, project_id: str, script_path: str
) -> SubmissionPreview:
    """Read a project-relative script in bounded chunks and preview it locally."""
    return create_submission_preview(script_path, await read_remote_script(remote_files, project_id, script_path))


async def read_remote_script(remote_files: Any, project_id: str, script_path: str) -> str:
    _validate_path(script_path)
    offset = 0
    chunks: list[str] = []
    while True:
        result = await remote_files.read(project_id, script_path, offset=offset)
        if result.size > MAX_SCRIPT_BYTES:
            raise ValueError("Slurm script exceeds the 1 MiB preview limit")
        chunks.append(result.content)
        if result.next_offset is None:
            return "".join(chunks)
        offset = result.next_offset


def _validate_path(path: str) -> None:
    if not path or "\x00" in path or "\n" in path or "\r" in path:
        raise ValueError("Slurm script path must not contain NUL or newline characters")
    if path.startswith("/") or ".." in path.split("/"):
        raise ValueError("Slurm script path is outside the remote project")


def _validate_resources(resources: SlurmResources) -> None:
    for key in ("--cpus-per-task", "--nodes", "--ntasks"):
        if key in resources.raw_numeric_requests:
            field = {"--cpus-per-task": resources.cpus_per_task, "--nodes": resources.nodes, "--ntasks": resources.ntasks}[key]
            if field is None:
                raise ValueError(f"Invalid numeric resource request: {key}")
    if "--gres" in resources.raw_numeric_requests and resources.gpus is None:
        raise ValueError("Invalid gpu resource request")
    if resources.memory is not None and not _MEMORY.fullmatch(resources.memory):
        raise ValueError("Invalid numeric resource request: --mem")
    if resources.time is not None and not _TIME.fullmatch(resources.time):
        raise ValueError("Invalid numeric resource request: --time")


def _warnings(resources: SlurmResources) -> list[str]:
    warnings: list[str] = []
    for value, message in (
        (resources.partition, "No partition requested."),
        (resources.output, "No output log path requested."),
        (resources.error, "No error log path requested."),
        (resources.time, "No time limit requested."),
        (resources.memory, "No memory request specified."),
        (resources.entry_command, "No entry command found."),
    ):
        if not value:
            warnings.append(message)
    return warnings
