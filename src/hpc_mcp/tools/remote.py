from typing import Any

from fastmcp import FastMCP

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure
from hpc_mcp.remote.files import READ_LIMIT_BYTES, SEARCH_MATCH_LIMIT
from hpc_mcp.remote.patch import TextEdit


def register_remote_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool
    async def remote_list(project_id: str, relative_path: str = ".") -> Any:
        """List a directory within a registered remote project."""
        try:
            return await container.remote_files.list(project_id=project_id, relative_path=relative_path)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_stat(project_id: str, relative_path: str) -> Any:
        """Return metadata for a project-relative remote path."""
        try:
            return await container.remote_files.stat(project_id=project_id, relative_path=relative_path)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_read(project_id: str, relative_path: str, offset: int = 0, limit: int = READ_LIMIT_BYTES) -> Any:
        """Read bounded UTF-8 text with a SHA-256 revision for optimistic editing."""
        try:
            return await container.remote_files.read(project_id=project_id, relative_path=relative_path, offset=offset, limit=limit)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_search(project_id: str, pattern: str, relative_path: str = ".", max_matches: int = SEARCH_MATCH_LIMIT) -> Any:
        """Search for literal text inside the remote project with bounded results."""
        try:
            return await container.remote_files.search(project_id=project_id, pattern=pattern, relative_path=relative_path, max_matches=max_matches)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_apply_patch(project_id: str, relative_path: str, expected_sha256: str, edits: list[TextEdit]) -> Any:
        """Apply text edits only when the remote revision and match counts agree."""
        try:
            return await container.remote_patch.apply(project_id=project_id, relative_path=relative_path, expected_sha256=expected_sha256, edits=edits)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_upload(project_id: str, local_relative_path: str, remote_relative_path: str | None = None) -> Any:
        """Upload a file between paths confined to the registered project roots."""
        try:
            return await container.remote_files.upload(project_id=project_id, local_relative_path=local_relative_path, remote_relative_path=remote_relative_path)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_download(project_id: str, remote_relative_path: str, local_relative_path: str | None = None) -> Any:
        """Download a remote file into the registered local project."""
        try:
            return await container.remote_files.download(project_id=project_id, remote_relative_path=remote_relative_path, local_relative_path=local_relative_path)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def remote_execute(project_id: str, program: str, args: list[str], relative_cwd: str = ".", environment: dict[str, str] | None = None, timeout_seconds: float = 30) -> Any:
        """Execute structured program arguments under the remote project and command policy."""
        try:
            return await container.execution.execute(project_id=project_id, program=program, args=args, relative_cwd=relative_cwd, environment=environment, timeout_seconds=timeout_seconds)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)
