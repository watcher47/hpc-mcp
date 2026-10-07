from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import BeforeValidator

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure


def _strict_timeout(value: Any) -> int:
    # FastMCP's lax TypeAdapter call overrides StrictInt; validate before coercion.
    if type(value) is not int:
        raise ValueError("timeout_ms must be an integer")
    return value


TimeoutMilliseconds = Annotated[int, BeforeValidator(_strict_timeout)]


def register_ssh_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool
    async def server_list() -> Any:
        """List configured SSH aliases and public endpoint information without connecting."""
        try:
            return await container.general_ssh.server_list()
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def ssh_execute(cmd_string: str, server_alias: str = "default",
                          directory: str | None = None, timeout_ms: TimeoutMilliseconds | None = None) -> Any:
        """Run a config-allowlisted command in an allowed directory; submit jobs with slurm_submit."""
        try:
            return await container.general_ssh.execute(cmd_string=cmd_string,
                server_alias=server_alias, directory=directory, timeout_ms=timeout_ms)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def ssh_upload(local_path: str, remote_path: str,
                         server_alias: str = "default") -> Any:
        """Upload one regular file between configured allowed roots, replacing the destination."""
        try:
            return await container.general_ssh.upload(local_path=local_path,
                remote_path=remote_path, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def ssh_download(remote_path: str, local_path: str,
                           server_alias: str = "default") -> Any:
        """Download one regular file between configured allowed roots, replacing the destination."""
        try:
            return await container.general_ssh.download(remote_path=remote_path,
                local_path=local_path, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)
