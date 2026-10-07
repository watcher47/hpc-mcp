from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_config_path, user_data_path

from hpc_mcp.errors import HpcMcpError


@dataclass(frozen=True)
class AppPaths:
    config_directory: Path
    data_directory: Path

    @classmethod
    def for_user(cls) -> AppPaths:
        return cls(
            config_directory=user_config_path("hpc-codex-mcp", appauthor=False),
            data_directory=(Path.home() / ".hpc-codex-mcp" / "state"
                            if os.name == "nt" else user_data_path("hpc-codex-mcp")),
        )

    @property
    def servers_file(self) -> Path:
        return self.config_directory / "servers.json"

    @property
    def host_trust_file(self) -> Path:
        return self.data_directory / "host-trust.sqlite3"

    @property
    def agent_database(self) -> Path:
        return self.data_directory / "agent.sqlite3"


def resolve_servers_path(
    explicit: str | Path | None = None, environ: Mapping[str, str] | None = None
) -> Path:
    """Resolve the profile path with CLI > environment > platform precedence."""

    environment = os.environ if environ is None else environ
    candidate = explicit or environment.get("HPC_MCP_CONFIG")
    path = (
        Path(candidate).expanduser() if candidate else AppPaths.for_user().servers_file
    )
    resolved = path.resolve()
    _reject_managed_project_location(resolved)
    return resolved


def _reject_managed_project_location(path: Path) -> None:
    if any(part.casefold().startswith("onedrive") for part in path.parts):
        raise HpcMcpError(
            "CONFIG_LOCATION_UNSAFE", "SSH configuration must not be stored in OneDrive"
        )
    user_home = Path.home().resolve()
    for parent in (path.parent, *path.parent.parents):
        # A user home can itself be a broad dotfiles repository. Treat only a
        # narrower checkout as a project location, otherwise every safe path
        # below AppData would be rejected on such machines.
        if parent == user_home:
            continue
        if (parent / ".git").exists():
            raise HpcMcpError(
                "CONFIG_LOCATION_UNSAFE",
                "SSH configuration must not be stored in a Git project",
            )
