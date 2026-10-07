"""Application composition for project-scoped services; no connections at import time."""
from __future__ import annotations

import posixpath
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from hpc_mcp.config.paths import AppPaths, resolve_servers_path
from hpc_mcp.agent.store import AgentStore
from hpc_mcp.agent.service import WatchService, watch_command
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import RemotePathGuard
from hpc_mcp.project.registry import ProjectRecord, ProjectRegistry
from hpc_mcp.project.sync import ProjectSyncService
from hpc_mcp.remote.files import RemoteFileService, _redact_io_errors
from hpc_mcp.remote.patch import RemotePatchService
from hpc_mcp.slurm.preview import preview_remote_submission
from hpc_mcp.slurm.query import SlurmQueryService
from hpc_mcp.slurm.submit import SlurmSubmitService
from hpc_mcp.ssh.client import SshClient
from hpc_mcp.ssh.general import GeneralSshService
from hpc_mcp.ssh.policy import CommandPolicy, RemoteCommand
from hpc_mcp.ssh.protocol import SshClientProtocol
from hpc_mcp.ssh.trust import HostTrustStore


def _project(registry: ProjectRegistry, project_id: str) -> ProjectRecord:
    record = registry.get(project_id)
    if record is None:
        raise HpcMcpError("PROJECT_NOT_FOUND", "Project is not registered")
    return record


class ConnectionService:
    def __init__(self, ssh: Callable[[str], SshClientProtocol]) -> None:
        self._ssh = ssh

    async def test(self, server_alias: str) -> dict[str, Any]:
        client = self._ssh(server_alias)
        home = await client.realpath(".")
        capabilities = {}
        for program in ("sbatch", "squeue", "sacct"):
            result = await client.run(RemoteCommand("which", [program], cwd=home))
            capabilities[program] = result.exit_status == 0 and bool(result.stdout.strip())
        return {"ssh_authenticated": True, "home_directory": home,
                "capabilities": capabilities, "ready": all(capabilities.values())}


class ProjectService:
    def __init__(self, configs: ConfigRepository, registry: ProjectRegistry,
                 ssh: Callable[[str], SshClientProtocol],
                 sync: Callable[[str], ProjectSyncService]) -> None:
        self._configs, self._registry, self._ssh, self._sync = configs, registry, ssh, sync

    @_redact_io_errors
    async def ensure(self, local_root: str, server_alias: str) -> ProjectRecord:
        root = Path(local_root).expanduser().resolve()
        if not root.is_dir():
            raise HpcMcpError("LOCAL_PROJECT_NOT_FOUND", "Local project directory does not exist")
        config = self._configs.load_cluster(server_alias)
        client = self._ssh(server_alias)
        base = config.remote_projects_root
        if base == "~" or base.startswith("~/"):
            base = posixpath.join(await client.realpath("."), base[2:] if base != "~" else "")
        if not base.startswith("/") or ".." in PurePosixPath(base).parts:
            raise HpcMcpError("CONFIG_INVALID", "Remote projects root must be absolute or home-relative")
        canonical_base = posixpath.normpath(await client.realpath(base))
        if not canonical_base.startswith("/"):
            raise HpcMcpError("CONFIG_INVALID", "Remote projects root must be canonical")
        record = self._registry.ensure(root, server_alias, canonical_base)
        async with client.sftp() as sftp:
            await sftp.makedirs(record.remote_root, exist_ok=True)
        await RemotePathGuard.resolve(client, record, ".")
        return record

    async def status(self, project_id: str) -> ProjectRecord:
        return _project(self._registry, project_id)

    @_redact_io_errors
    async def sync(self, project_id: str) -> Any:
        record = _project(self._registry, project_id)
        return await self._sync(record.server_alias).execute(record)

    @_redact_io_errors
    async def list_remote_changes(self, project_id: str) -> Any:
        record = _project(self._registry, project_id)
        return await self._sync(record.server_alias).plan(record)


class ProjectServiceRouter:
    """Bind a stateless domain service to the project's configured SSH client per call."""
    def __init__(self, registry: ProjectRegistry, ssh: Callable[[str], SshClientProtocol],
                 factory: Callable[..., Any], methods: set[str]) -> None:
        self._registry, self._ssh, self._factory, self._methods = registry, ssh, factory, methods

    def __getattr__(self, name: str) -> Any:
        if name not in self._methods:
            raise AttributeError(name)

        async def invoke(project_id: str, *args: Any, **kwargs: Any) -> Any:
            record = _project(self._registry, project_id)
            service = self._factory(self._registry, self._ssh(record.server_alias))
            return await getattr(service, name)(project_id, *args, **kwargs)
        return invoke


class ExecutionService:
    def __init__(self, registry: ProjectRegistry, ssh: Callable[[str], SshClientProtocol]) -> None:
        self._registry, self._ssh = registry, ssh

    async def execute(self, project_id: str, program: str, args: list[str],
                      relative_cwd: str, environment: dict[str, str] | None,
                      timeout_seconds: float) -> Any:
        record = _project(self._registry, project_id)
        client = self._ssh(record.server_alias)
        cwd = await RemotePathGuard.resolve(client, record, relative_cwd)
        try:
            command = RemoteCommand.for_user(
                program, args, cwd, environment, timeout_seconds
            )
        except ValueError:
            raise HpcMcpError("INVALID_COMMAND", "Remote command arguments are invalid") from None
        return await client.run(CommandPolicy().validate(command))


class PreviewService:
    def __init__(self, files: ProjectServiceRouter) -> None:
        self._files = files

    async def preview(self, project_id: str, script_path: str) -> Any:
        try:
            return await preview_remote_submission(self._files, project_id, script_path)
        except ValueError:
            raise HpcMcpError("SLURM_SCRIPT_INVALID", "Slurm script is invalid") from None


class SubmissionService:
    """Compose idempotent submission with local registration for job/log queries."""
    def __init__(self, registry: ProjectRegistry, ssh: Callable[[str], SshClientProtocol],
                 files: ProjectServiceRouter, preview: PreviewService,
                 config_file: Path | None = None) -> None:
        self._registry, self._ssh, self._files, self._preview = registry, ssh, files, preview
        self._config_file = config_file

    async def submit(self, project_id: str, script_path: str, request_id: UUID) -> Any:
        record = _project(self._registry, project_id)
        preview = await self._preview.preview(project_id, script_path)
        service = SlurmSubmitService(self._registry, self._ssh(record.server_alias), self._files)
        try:
            receipt = await service.submit(project_id, script_path, request_id)
        except ValueError:
            raise HpcMcpError("SLURM_SCRIPT_INVALID", "Slurm script is invalid") from None
        stdout, stderr = _registered_log_paths(preview, receipt.job_id)
        try:
            self._registry.register_job(project_id, receipt.job_id, stdout, stderr)
        except (HpcMcpError, sqlite3.Error):
            raise HpcMcpError(
                "JOB_REGISTRATION_FAILED", "Job was submitted but local registration failed",
                details={"job_id": receipt.job_id, "request_id": str(request_id),
                         "recovery": "Retry with the same request ID to recover this receipt."},
            ) from None
        return replace(receipt, stdout_path=stdout, stderr_path=stderr,
                       watch_command=watch_command(record.server_alias, receipt.job_id,
                                                   project_id, self._config_file))


def _log_path(template: str | None, job_id: str) -> str | None:
    if template is None:
        return None
    value = template.replace("%j", job_id)
    # Unknown scheduler substitutions and external paths must not become readable logs.
    path = PurePosixPath(value)
    if "%" in value or path.is_absolute() or ".." in path.parts:
        return None
    return str(path)


def _registered_log_paths(preview: Any, job_id: str) -> tuple[str | None, str | None]:
    """Return only log paths whose submission-time location is deterministic."""
    directives = preview.resources.extra_directives
    if any(key in directives for key in {"--array", "-a", "--chdir", "-D"}):
        return None, None
    stdout = _log_path(preview.resources.output or "slurm-%j.out", job_id)
    stderr = _log_path(preview.resources.error, job_id) if preview.resources.error else stdout
    return stdout, stderr


@dataclass
class ServiceContainer:
    configs: ConfigRepository
    registry: ProjectRegistry
    ssh: Callable[[str], SshClientProtocol]
    project_sync: Callable[[str], ProjectSyncService]
    connection: ConnectionService
    projects: ProjectService
    remote_files: ProjectServiceRouter
    remote_patch: ProjectServiceRouter
    execution: ExecutionService
    slurm_preview: PreviewService
    slurm_submit: SubmissionService
    slurm_query: SlurmQueryService
    general_ssh: GeneralSshService
    agent_store: AgentStore
    slurm_watch: WatchService


def build_container(
    paths: AppPaths | None = None, config_file: str | Path | None = None
) -> ServiceContainer:
    paths = paths or AppPaths.for_user()
    paths.data_directory.mkdir(parents=True, exist_ok=True)
    servers_file = resolve_servers_path(config_file) if config_file else paths.servers_file
    configs = ConfigRepository(servers_file)
    host_trust = HostTrustStore(paths.host_trust_file)
    registry = ProjectRegistry(paths.data_directory / "projects.sqlite3")
    agent_store = AgentStore(paths.agent_database)

    def ssh(alias: str) -> SshClientProtocol:
        return SshClient(configs.load_cluster(alias), host_trust)

    def sync(alias: str) -> ProjectSyncService:
        return ProjectSyncService(ssh(alias))

    files = ProjectServiceRouter(registry, ssh, RemoteFileService,
                                 {"list", "stat", "read", "search", "upload", "download"})
    patch = ProjectServiceRouter(registry, ssh, RemotePatchService, {"apply"})
    preview = PreviewService(files)
    return ServiceContainer(
        configs=configs, registry=registry, ssh=ssh, project_sync=sync,
        connection=ConnectionService(ssh), projects=ProjectService(configs, registry, ssh, sync),
        remote_files=files, remote_patch=patch, execution=ExecutionService(registry, ssh),
        slurm_preview=preview, slurm_submit=SubmissionService(registry, ssh, files, preview, servers_file),
        slurm_query=SlurmQueryService(registry, ssh, username=lambda alias: configs.load_cluster(alias).username),
        general_ssh=GeneralSshService(configs, ssh),
        agent_store=agent_store, slurm_watch=WatchService(registry, agent_store),
    )
