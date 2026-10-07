from typing import Any

from fastmcp import FastMCP

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure


def register_project_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool
    async def project_ensure(local_root: str, server_alias: str = "default") -> Any:
        """Register a local project and ensure its remote project directory exists."""
        try:
            return await container.projects.ensure(local_root=local_root, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def project_status(project_id: str) -> Any:
        """Return the registered project mapping."""
        try:
            return await container.projects.status(project_id=project_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def project_sync(project_id: str) -> Any:
        """Incrementally upload project changes while preserving conflicts."""
        try:
            return await container.projects.sync(project_id=project_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def project_list_remote_changes(project_id: str) -> Any:
        """Plan synchronization and report remote changes without uploading."""
        try:
            return await container.projects.list_remote_changes(project_id=project_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)
