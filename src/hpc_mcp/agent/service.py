"""Register watches only in the originating terminal; MCP may only verify them."""
from __future__ import annotations

import os
import re
import shlex
from collections.abc import Mapping
from pathlib import Path

from hpc_mcp.agent.store import AgentStore, JobWatch
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry


def watch_command(alias: str, job_id: str, project_id: str, config_file: Path | None = None) -> str:
    args = ["hpc-agent"]
    if config_file is not None:
        args += ["--config-file", str(config_file)]
    args += ["watch", "--server", alias, "--job-id", job_id, "--project-id", project_id]
    if os.name != "nt":
        return shlex.join(args)
    # The supported Windows terminal is PowerShell. Single quotes suppress
    # interpolation, including paths containing dollar signs and backticks.
    return " ".join(arg if re.fullmatch(r"[A-Za-z0-9_./:-]+", arg)
                    else "'" + arg.replace("'", "''") + "'" for arg in args)


class WatchService:
    def __init__(self, registry: ProjectRegistry, store: AgentStore) -> None:
        self.registry, self.store = registry, store

    def register(self, alias: str, job_id: str, project_id: str, *,
                 environ: Mapping[str, str] | None = None, cwd: Path | None = None) -> JobWatch:
        environment = os.environ if environ is None else environ
        thread = environment.get("CODEX_THREAD_ID", "").strip()
        session = environment.get("CODEX_SESSION_ID", "").strip()
        if not thread or not session:
            raise HpcMcpError("CODEX_CONTEXT_MISSING", "Run watch in the originating Codex terminal with both context environment variables")
        project = self.registry.get(project_id)
        job = self.registry.get_job(alias, job_id)
        if project is None or job is None or job.project_id != project_id or project.server_alias != alias:
            raise HpcMcpError("JOB_NOT_REGISTERED", "Job is not registered to this server and project")
        directory = (cwd or Path.cwd()).resolve()
        if not directory.is_relative_to(project.local_root.resolve()):
            raise HpcMcpError("WATCH_CWD_MISMATCH", "Run watch from the registered local project")
        return self.store.register_watch(server_alias=alias, job_id=job_id, project_id=project_id,
            thread_id=thread, session_id=session, cwd=str(directory))

    async def verify(self, job_id: str, server_alias: str = "default") -> dict:
        job = self.registry.get_job(server_alias, job_id)
        if job is None:
            raise HpcMcpError("JOB_NOT_REGISTERED", "Job is not registered")
        watch = self.store.find_watch(server_alias, job_id)
        if watch is None or watch.project_id != job.project_id:
            raise HpcMcpError("WATCH_NOT_REGISTERED", "Run the returned hpc-agent watch command in the originating Codex terminal")
        pending = any(e.watch_id == watch.id and e.delivery_state != "DELIVERED" for e in self.store.events())
        return {"watch_id": watch.id, "server_alias": server_alias, "job_id": job_id,
                "project_id": watch.project_id, "active": bool(watch.active),
                "last_state": watch.last_state, "terminal_event_pending": pending}
