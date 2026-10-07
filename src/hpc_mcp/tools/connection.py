from typing import Any

from fastmcp import FastMCP

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure


def register_connection_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool
    async def server_test_connection(server_alias: str = "default") -> Any:
        """Check configured SSH connectivity and Slurm command availability."""
        try:
            return await container.connection.test(server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)
