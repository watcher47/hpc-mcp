from typing import Any
from uuid import UUID

from fastmcp import FastMCP

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure


def register_slurm_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool
    async def slurm_watch_job(job_id: str, server_alias: str = "default") -> Any:
        """Verify local handoff registration. Run hpc-agent watch in the original Codex terminal first."""
        try:
            return await container.slurm_watch.verify(job_id=job_id, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_preview(project_id: str, script_path: str) -> Any:
        """Preview a remote batch script without submitting a job."""
        try:
            return await container.slurm_preview.preview(project_id=project_id, script_path=script_path)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_submit(project_id: str, script_path: str, request_id: UUID) -> Any:
        """Submit a validated batch script idempotently; reuse the request UUID on retries."""
        try:
            return await container.slurm_submit.submit(project_id=project_id, script_path=script_path, request_id=request_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_get_job(job_id: str) -> Any:
        """Inspect one registered job using the queue and accounting records."""
        try:
            return await container.slurm_query.get_job(job_id=job_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_list_jobs() -> Any:
        """Inspect locally registered jobs only."""
        try:
            return await container.slurm_query.list_jobs()
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_read_log(job_id: str, stream: str = "stdout", offset: int | None = None) -> Any:
        """Read a bounded chunk of a registered job log; default to its tail."""
        try:
            return await container.slurm_query.read_log(job_id=job_id, stream=stream, offset=offset)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_list_artifacts(job_id: str) -> Any:
        """List bounded artifacts within a registered job's project output directory."""
        try:
            return await container.slurm_query.list_artifacts(job_id=job_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)
