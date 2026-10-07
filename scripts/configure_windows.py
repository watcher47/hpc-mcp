"""New-profile local dialog. Credentials never enter command arguments or logs."""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.config.paths import AppPaths, resolve_servers_path
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.setup.windows import WindowsStartup, run_command

SITE_HOST = "10.29.114.128"
PLACEHOLDERS = {"ssh_username", "your_username", "username", "replace_with_password", "password", "your_password"}


def build_profile(username: str, password: str, local_root: str, host: str = SITE_HOST, remote_root: str | None = None) -> ClusterConfig:
    root = Path(local_root).expanduser()
    if (not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,63}", username)
            or username.casefold() in PLACEHOLDERS
            or not password.strip() or password.casefold() in PLACEHOLDERS
            or not host.strip() or host.strip() == "COMPUTE_HOST"
            or not root.is_absolute() or not root.is_dir()
            or root.resolve() == Path(root.anchor)):
        raise HpcMcpError("SETUP_INPUT_INVALID", "Enter your own SSH identity and an existing absolute project directory")
    remote_root = remote_root or (f"/home/{username}/codex-projects" if host == SITE_HOST else "")
    if (not remote_root.startswith("/") or remote_root == "/"
            or any(part in {"", ".", ".."} for part in remote_root[1:].split("/"))
            or "\\" in remote_root or "\x00" in remote_root):
        raise HpcMcpError("SETUP_INPUT_INVALID", "Confirm an absolute remote project root for this cluster")
    try:
        return ClusterConfig(alias="default", host=host.strip(), username=username,
            password=password, allowedLocalPaths=[str(root.resolve())],
            remoteProjectsRoot=remote_root, allowedRemotePaths=[remote_root],
            whitelist=[r"^squeue(?: .*)?$", r"^sacct(?: .*)?$"],
            blacklist=[r"^rm(?: .*)?$", r"^shutdown(?: .*)?$"])
    except Exception:
        raise HpcMcpError("SETUP_INPUT_INVALID", "Configuration input is invalid") from None


def secure_file(path: Path) -> None:
    """Protect the empty inode before any credential bytes are written."""
    if os.name != "nt":
        path.chmod(0o600)
        return
    sid = WindowsStartup()._identity()
    # mkstemp's new file has inherited ACLs only. Remove inheritance, then grant
    # exactly this SID; do not preserve broad inherited permissions.
    result = run_command(["icacls", str(path), "/inheritance:r", "/grant:r", f"*{sid}:(F)"])
    if result.returncode:
        raise OSError("ACL restriction failed")


def publish_file(temporary: Path, destination: Path) -> None:
    os.link(temporary, destination)


def new_destination(destination=None) -> Path:
    # Inspect the selected lexical leaf BEFORE resolve_servers_path follows it.
    # A dangling link is still an existing destination and must be preserved.
    selected = destination or os.environ.get("HPC_MCP_CONFIG") or AppPaths.for_user().servers_file
    lexical = Path(selected).expanduser()
    if lexical.exists() or lexical.is_symlink():
        raise HpcMcpError("CONFIG_EXISTS", "Existing configuration was preserved")
    path = resolve_servers_path(lexical)
    if path.exists() or path.is_symlink():
        raise HpcMcpError("CONFIG_EXISTS", "Existing configuration was preserved")
    return path


def configure(destination, username, password, local_root, host=SITE_HOST, remote_root=None) -> str:
    if password is None:
        return "cancelled"
    path = new_destination(destination)
    profile = build_profile(username, password, local_root, host, remote_root)
    temporary = None
    descriptor = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=".hpc-config-", dir=path.parent)
        temporary = Path(name)
        secure_file(temporary)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = None
            json.dump({"default": ConfigRepository._config_payload(profile)}, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        publish_file(temporary, path)
    except Exception:
        raise HpcMcpError("CONFIG_CREATE_FAILED", "Secure configuration creation failed; setup is incomplete") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return "completed"


def dialog(args) -> str:
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.title("hpc-mcp — local SSH configuration")
    entries = {}
    for row, (name, label, value) in enumerate([
        ("host", "Cluster host (site preset)", args.host),
        ("username", "Your SSH username", args.username),
        ("local_root", "Authorized local project directory", args.local_root),
        ("remote_root", "Confirm remote project root (site home convention)", args.remote_root or (f"/home/{args.username}/codex-projects" if args.host == SITE_HOST else "")),
        ("password", "SSH password (saved only locally)", ""),
    ]):
        tk.Label(root, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=5)
        entry = tk.Entry(root, width=55, show="*" if name == "password" else "")
        entry.insert(0, value)
        entry.grid(row=row, column=1, padx=10, pady=5)
        entries[name] = entry
    outcome = "cancelled"
    def save():
        nonlocal outcome
        try:
            outcome = configure(args.config_file, entries["username"].get(), entries["password"].get(),
                entries["local_root"].get(), entries["host"].get(), entries["remote_root"].get())
        except HpcMcpError as error:
            messagebox.showerror("Setup incomplete", error.message)
            return
        root.destroy()
    tk.Button(root, text="Save new configuration", command=save).grid(row=5, column=1, pady=10)
    tk.Button(root, text="Cancel", command=root.destroy).grid(row=5, column=0, pady=10)
    root.mainloop()
    return outcome


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-file", type=Path)
    parser.add_argument("--username", required=True)
    parser.add_argument("--local-root", required=True)
    parser.add_argument("--host", default=SITE_HOST)
    parser.add_argument("--remote-root")
    args = parser.parse_args(argv)
    if os.name != "nt":
        print("incomplete: this dialog requires Windows")
        return 1
    try:
        new_destination(args.config_file)
        outcome = dialog(args)
        print(outcome)
        return 0 if outcome == "completed" else 2
    except Exception:
        print("incomplete: local GUI/configuration unavailable; use official full Python with tkinter")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
