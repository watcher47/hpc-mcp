from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SlurmResources:
    """Resource requests extracted from a Slurm batch script."""

    job_name: str | None = None
    partition: str | None = None
    gpus: int | None = None
    cpus_per_task: int | None = None
    memory: str | None = None
    time: str | None = None
    output: str | None = None
    error: str | None = None
    nodes: int | None = None
    ntasks: int | None = None
    entry_command: str | None = None
    extra_directives: dict[str, str | None] = field(default_factory=dict)
    raw_numeric_requests: dict[str, str] = field(default_factory=dict)
    gpu_gres: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class JobState:
    """One row from the fixed-format ``squeue`` query."""

    job_id: str
    state: str
    name: str
    partition: str
    elapsed: str
    node_list: str


@dataclass(frozen=True, slots=True)
class SubmissionPreview:
    """A validation-only description of a prospective Slurm submission."""

    script_path: str
    resources: SlurmResources
    entry_command: str | None
    warnings: list[str] = field(default_factory=list)
    executable: bool = False
    gpu: dict | None = None
