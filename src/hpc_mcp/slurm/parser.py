from __future__ import annotations

import re
import shlex

from hpc_mcp.slurm.models import JobState, SlurmResources


_DIRECTIVE = re.compile(r"^\s*#SBATCH\s+(?P<body>.+?)\s*$")
_KNOWN = {
    "--job-name": "job_name",
    "--partition": "partition",
    "--cpus-per-task": "cpus_per_task",
    "--mem": "memory",
    "--time": "time",
    "--output": "output",
    "--error": "error",
    "--nodes": "nodes",
    "--ntasks": "ntasks",
}
_SHORT = {"-J": "--job-name", "-p": "--partition", "-c": "--cpus-per-task", "-t": "--time", "-o": "--output", "-e": "--error"}
_NUMERIC = {"--cpus-per-task", "--nodes", "--ntasks"}


def parse_sbatch_directives(script: str) -> SlurmResources:
    """Parse SBATCH comments only; this function never invokes Slurm."""
    values: dict[str, object] = {"extra_directives": {}, "raw_numeric_requests": {}}
    entry_command: str | None = None
    for line in script.splitlines():
        match = _DIRECTIVE.match(line)
        if match:
            key, value = _directive_parts(match.group("body"))
            key = _SHORT.get(key, key)
            if key == "--gres":
                values["gpus"] = _gpu_count(value)
                if value is not None:
                    values["raw_numeric_requests"]["--gres"] = value  # type: ignore[index]
            elif key in _KNOWN:
                field = _KNOWN[key]
                if key in _NUMERIC and value is not None:
                    values["raw_numeric_requests"][key] = value  # type: ignore[index]
                    values[field] = _positive_integer_or_none(value)
                else:
                    values[field] = value
            else:
                values["extra_directives"][key] = value  # type: ignore[index]
            continue
        stripped = line.strip()
        if entry_command is None and stripped and not stripped.startswith("#"):
            entry_command = stripped
    values["entry_command"] = entry_command
    return SlurmResources(**values)  # type: ignore[arg-type]


def parse_squeue_rows(output: str) -> list[JobState]:
    """Parse ``squeue -o '%i|%T|%j|%P|%M|%N'`` output with fixed fields."""
    rows: list[JobState] = []
    for line in output.splitlines():
        if not line:
            continue
        fields = line.split("|")
        if len(fields) != 6:
            raise ValueError("squeue rows must contain exactly six pipe-delimited fields")
        rows.append(JobState(*fields))
    return rows


def _directive_parts(body: str) -> tuple[str, str | None]:
    parts = shlex.split(body, comments=False, posix=True)
    if not parts:
        return "", None
    key, separator, value = parts[0].partition("=")
    if separator:
        return key, value
    return key, " ".join(parts[1:]) if len(parts) > 1 else None


def _positive_integer_or_none(value: str) -> int | None:
    return int(value) if value.isdecimal() and int(value) > 0 else None


def _gpu_count(value: str | None) -> int | None:
    if value is None:
        return None
    for item in value.split(","):
        parts = item.split(":")
        if parts[0] == "gpu" and len(parts) >= 2:
            return _positive_integer_or_none(parts[-1])
    return 0
