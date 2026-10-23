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


async def verify_local_context(*, environ=None, cwd=None, schema_loader=None, client_factory=None):
    """Read the real originating task before making any durable watch binding."""
    from hpc_mcp.agent.app_server import AppServerClient, generate_schema
    environment = os.environ if environ is None else environ
    thread = environment.get("CODEX_THREAD_ID", "").strip()
    session = environment.get("CODEX_SESSION_ID", "").strip()
    if not thread or not session:
        raise HpcMcpError("CODEX_CONTEXT_MISSING", "Run in the originating local Codex terminal with both context variables")
    directory = (cwd or Path.cwd()).resolve()
    schema = await (schema_loader or generate_schema)(route="queue")
    async with (client_factory or AppServerClient)(schema) as client:
        result = await client.request("thread/read", {"threadId": thread, "includeTurns": False})
    native = result.get("thread") if isinstance(result, dict) else None
    if (not isinstance(native, dict) or native.get("id") != thread
            or native.get("sessionId") != session or not isinstance(native.get("turns"), list)
            or not isinstance(native.get("status"), dict) or not isinstance(native.get("cwd"), str)
            or not directory.is_relative_to(Path(native["cwd"]).resolve())):
        raise HpcMcpError("CODEX_THREAD_CONTEXT_INVALID", "Native task identity, session or directory does not match this terminal")
    return {"thread_id": thread, "session_id": session, "cwd": str(directory)}


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

    def recover(self, alias, job_id, project_id, event_id, expected_thread_id, *, context):
        """Explicitly recover a never-sent event to a verified originating terminal."""
        watch = self.store.find_watch(alias, job_id)
        project = self.registry.get(project_id)
        job = self.registry.get_job(alias, job_id)
        if (watch is None or project is None or job is None or watch.project_id != project_id
                or job.project_id != project_id or project.server_alias != alias
                or not Path(context["cwd"]).resolve().is_relative_to(project.local_root.resolve())):
            raise HpcMcpError("WATCH_RECOVERY_BLOCKED", "Recovery job, project or directory does not match")
        import time
        return self.store.recover_watch_context(watch_id=watch.id, event_id=event_id,
            expected_thread_id=expected_thread_id, thread_id=context["thread_id"],
            session_id=context["session_id"], cwd=context["cwd"], now=time.time())
