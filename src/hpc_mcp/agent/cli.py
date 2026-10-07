"""Independent local agent CLI. Never accept Codex thread/session IDs as flags."""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

import typer

from hpc_mcp.agent.monitor import Monitor
from hpc_mcp.agent.delivery import manual_recovery
from hpc_mcp.agent.queued_delivery import QueueDeliveryService as DeliveryService
from hpc_mcp.agent.queue_store import QueueStore
from hpc_mcp.config.paths import resolve_servers_path
from hpc_mcp.container import build_container
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.setup.windows import InstanceLock, WindowsStartup

app = typer.Typer(no_args_is_help=True)


@contextmanager
def _errors():
    try:
        yield
    except HpcMcpError as error:
        typer.echo(f"Agent error [{error.code}]: {error.message}", err=True)
        raise typer.Exit(1) from None
    except (OSError, sqlite3.Error):
        typer.echo("Agent error [AGENT_STORAGE_FAILED]: Could not access local agent state", err=True)
        raise typer.Exit(1) from None


@app.callback()
def main(context: typer.Context,
         config_file: Annotated[Path | None, typer.Option("--config-file")] = None) -> None:
    """Monitor registered Slurm jobs without keeping an MCP session alive."""
    with _errors():
        path = resolve_servers_path(config_file)
        context.obj = path if context.invoked_subcommand in {"install-startup", "uninstall-startup"} else build_container(config_file=path)


@app.command()
def watch(context: typer.Context, server: Annotated[str, typer.Option("--server")],
          job_id: Annotated[str, typer.Option("--job-id")],
          project_id: Annotated[str, typer.Option("--project-id")]) -> None:
    """Bind an already submitted job to this Codex terminal's context."""
    with _errors():
        record = context.obj.slurm_watch.register(server, job_id, project_id)
        typer.echo(json.dumps(asdict(record)))


@app.command()
def status(context: typer.Context) -> None:
    """Show durable local watches, completion events, and server retry status."""
    with _errors():
        store = context.obj.agent_store
        typer.echo(json.dumps({"watches": [asdict(w) for w in store.watches()],
            "events": [asdict(e) | {"diagnostics": _diagnostic_summary(store, e.id),
                "queue": _queue_summary(store, e.id)} for e in store.events()],
            "servers": [{k: v for k, v in asdict(s).items() if k != "config_version"}
                        for s in store.server_states()]}))


def _diagnostic_summary(store, event_id):
    report = store.diagnostics(event_id, limit=1)
    return {k: report[k] for k in ("total_attempt_count", "retained_count", "gap_count")} | {
        "last_attempt": report["attempts"][0] if report["attempts"] else None}


def _queue_summary(store, event_id):
    receipt = QueueStore(store).get(event_id)
    return asdict(receipt) if receipt else None


@app.command()
def diagnostics(context: typer.Context, event_id: Annotated[str, typer.Option("--event-id")],
                limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 20) -> None:
    """Read bounded safe delivery evidence for one completion event."""
    with _errors():
        store = context.obj.agent_store
        typer.echo(json.dumps(store.diagnostics(event_id, limit=limit) | {
            "queue": _queue_summary(store, event_id)}))


@app.command("retry-server")
def retry_server(context: typer.Context, server: Annotated[str, typer.Option("--server")]) -> None:
    """Explicitly clear a server's authentication pause or retry backoff."""
    with _errors():
        context.obj.agent_store.retry_server(server)
        typer.echo("Server retry enabled")


@app.command("manual-recovery")
def recovery(context: typer.Context, event_id: Annotated[str, typer.Option("--event-id")]) -> None:
    """Print recovery instructions; never start a turn or acknowledge an event."""
    with _errors():
        typer.echo(json.dumps(manual_recovery(context.obj.agent_store, event_id)))


def config_version(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        raise HpcMcpError("CONFIG_READ_FAILED", "Could not read server configuration") from None


@app.command()
def run(context: typer.Context) -> None:
    """Poll Slurm, then hand off due completion events to the original Codex task."""
    with _errors():
        container = context.obj
        delivery = DeliveryService(container.agent_store)
        monitor = Monitor(container.agent_store, container.slurm_query,
                          lambda: config_version(container.configs.path),
                          after_poll=delivery.deliver_pending)
        async def run_managed():
            container.agent_store.recover_interrupted_handoffs(now=time.time())
            try:
                await monitor.run()
            finally:
                await delivery.aclose()
        try:
            with InstanceLock(container.agent_store.path.parent / "agent.lock"):
                asyncio.run(run_managed())
        except KeyboardInterrupt:
            pass


@app.command("install-startup")
def install_startup(context: typer.Context,
                    apply: Annotated[bool, typer.Option("--apply")] = False) -> None:
    """Plan current-user login startup; --apply registers it."""
    with _errors():
        if apply:
            python = Path(sys.executable)
            if os.name == "nt":
                python = python.with_name("pythonw.exe")
            WindowsStartup().install([str(python), "-m", "hpc_mcp.agent.cli", "--config-file", str(context.obj), "run"])
        typer.echo(json.dumps({"apply": apply, "action": "install-startup"}))


@app.command("uninstall-startup")
def uninstall_startup(context: typer.Context,
                      apply: Annotated[bool, typer.Option("--apply")] = False) -> None:
    """Plan startup removal; --apply removes only this user's managed task."""
    with _errors():
        if apply:
            WindowsStartup().uninstall()
        typer.echo(json.dumps({"apply": apply, "action": "uninstall-startup"}))


if __name__ == "__main__":
    app()
