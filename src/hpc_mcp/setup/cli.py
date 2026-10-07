from __future__ import annotations

import json
import asyncio
import os
import stat
import subprocess
import tempfile
import sqlite3
from pathlib import Path
from typing import Annotated
from contextlib import contextmanager

import typer

from hpc_mcp.config.paths import resolve_servers_path
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.setup.install import Installer

app = typer.Typer(no_args_is_help=True)

_TEMPLATE = {
    "default": {
        "host": "COMPUTE_HOST",
        "port": 22,
        "username": "SSH_USERNAME",
        "password": "REPLACE_WITH_PASSWORD",
        "privateKey": None,
        "passphrase": None,
        "remoteProjectsRoot": "~/codex-projects",
        "hostKeyPolicy": "tofu",
        "commandTimeoutMs": 30000,
        "connectionTimeoutMs": 15000,
        "sftpTimeoutMs": 300000,
        "keepaliveIntervalMs": 10000,
        "keepaliveCountMax": 3,
        "maxOutputBytes": 10485760,
        "whitelist": ["^squeue(?: .*)?$", "^sacct(?: .*)?$"],
        "blacklist": ["^rm(?: .*)?$", "^shutdown(?: .*)?$"],
        "allowedLocalPaths": ["REPLACE_WITH_ALLOWED_LOCAL_PATH"],
        "allowedRemotePaths": ["/home/SSH_USERNAME/codex-projects"],
        "transportMode": "exec",
    }
}


@app.callback()
def main(
    context: typer.Context,
    config_file: Annotated[Path | None, typer.Option("--config-file")] = None,
) -> None:
    """Initialize and validate local hpc-mcp configuration."""
    try:
        context.obj = resolve_servers_path(config_file)
    except HpcMcpError as error:
        typer.echo(f"Configuration error [{error.code}]: {error.message}", err=True)
        raise typer.Exit(code=1) from None


@app.command("init-config")
def init_config(context: typer.Context) -> None:
    path: Path = context.obj
    try:
        _write_new_template(path)
        permission_note = _restrict_to_current_user(path)
    except HpcMcpError as error:
        typer.echo(f"Initialization failed [{error.code}]: {error.message}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(f"Created SSH configuration template at {path}")
    if permission_note:
        typer.echo(permission_note, err=True)


@app.command("validate-config")
def validate_config(context: typer.Context) -> None:
    path: Path = context.obj
    try:
        count = len(ConfigRepository(path).load_all())
    except HpcMcpError as error:
        typer.echo(f"Validation failed [{error.code}]: {error.message}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(f"Configuration is valid ({count} profile(s)).")


@contextmanager
def _management_errors():
    try:
        yield
    except HpcMcpError as error:
        typer.echo(f"Setup failed [{error.code}]: {error.message}", err=True)
        raise typer.Exit(1) from None
    except (OSError, ValueError, sqlite3.Error):
        typer.echo("Setup failed [SETUP_FAILED]: Could not complete local setup", err=True)
        raise typer.Exit(1) from None


@app.command()
def install(context: typer.Context,
            apply: Annotated[bool, typer.Option("--apply")] = False,
            wheel: Annotated[Path | None, typer.Option("--wheel")] = None,
            source: Annotated[Path | None, typer.Option("--source")] = None,
            rollback_wheel: Annotated[Path | None, typer.Option("--rollback-wheel")] = None) -> None:
    """Plan installation; --apply stages and validates a wheel before registration."""
    with _management_errors():
        typer.echo(json.dumps(Installer(context.obj).install(apply=apply, wheel=wheel, source=source, rollback_wheel=rollback_wheel)))


@app.command()
def doctor(context: typer.Context,
           check_ssh: Annotated[bool, typer.Option("--check-ssh")] = False) -> None:
    """Validate local configuration and Codex schema; optionally probe SSH/Slurm."""
    with _management_errors():
        result = asyncio.run(Installer(context.obj).doctor(check_ssh=check_ssh))
        typer.echo(json.dumps(result))
        if not result["ready"]:
            raise typer.Exit(1)


@app.command()
def uninstall(context: typer.Context,
              apply: Annotated[bool, typer.Option("--apply")] = False) -> None:
    """Plan removal of managed integration; preserve all configuration and jobs."""
    with _management_errors():
        typer.echo(json.dumps(Installer(context.obj).uninstall(apply=apply)))


def _write_new_template(path: Path) -> None:
    if path.exists():
        raise HpcMcpError(
            "CONFIG_EXISTS", "Configuration already exists; it was not overwritten"
        )
    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(dir=path.parent)
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(_TEMPLATE, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # A hard link atomically publishes the complete file only if the target
        # does not exist. Unlike replace(), a racing creator cannot be clobbered.
        # Both names are in the same directory/filesystem; unsupported filesystems
        # fail safely rather than falling back to an overwriting publication.
        os.link(temporary_path, path)
    except FileExistsError:
        raise HpcMcpError(
            "CONFIG_EXISTS", "Configuration already exists; it was not overwritten"
        ) from None
    except OSError:
        raise HpcMcpError(
            "CONFIG_WRITE_FAILED", "Could not create configuration template"
        ) from None
    finally:
        if temporary_path is not None and temporary_path.exists():
            try:
                temporary_path.unlink()
            except OSError:
                pass


def _restrict_to_current_user(path: Path) -> str | None:
    if os.name != "nt":
        try:
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            return None
        except OSError:
            return (
                "Warning: could not restrict configuration permissions automatically."
            )

    username = os.environ.get("USERNAME")
    domain = os.environ.get("USERDOMAIN")
    account = f"{domain}\\{username}" if domain and username else username
    if not account:
        return "Warning: could not determine the current account for Windows ACL setup."
    try:
        completed = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{account}:(R,W)"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return "Warning: Windows ACL setup was unavailable; verify file permissions manually."
    if completed.returncode != 0:
        return "Warning: Windows ACL setup did not succeed; verify file permissions manually."
    return None
