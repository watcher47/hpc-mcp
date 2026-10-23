from typing import Annotated, Any
from uuid import UUID

from fastmcp import FastMCP
from pydantic import Field

from hpc_mcp.container import ServiceContainer
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure
from hpc_mcp.slurm.gpu import GpuSelection


def register_slurm_tools(mcp: FastMCP, container: ServiceContainer) -> None:
    @mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True})
    async def slurm_cancel_job(
        job_id: Annotated[str, Field(pattern=r"^[1-9][0-9]{0,9}(?:_(?:0|[1-9][0-9]{0,9}))?$")],
        request_id: UUID,
        server_alias: str | None = None,
    ) -> Any:
        """Normally cancel one job owned by the authenticated SSH user. Reuse the request UUID on retries.

        Omit server_alias for a uniquely registered job; otherwise select an existing configured
        alias explicitly. No lists, ranges, all-user cancellation, or custom signals. An accepted
        request is not a terminal state: inspect slurm_get_job afterward. Never automatically
        issue a new UUID after an uncertain outcome.
        """
        try:
            return await container.slurm_cancel.cancel_job(job_id=job_id, request_id=request_id, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_gpu_inventory(project_id: str) -> Any:
        """Inspect local-node NVIDIA GPU identities and occupancy without remote writes."""
        try:
            return await container.slurm_gpu.inventory(project_id=project_id)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_watch_job(job_id: str, server_alias: str = "default") -> Any:
        """Verify local handoff registration. Run hpc-agent watch in the original Codex terminal first."""
        try:
            return await container.slurm_watch.verify(job_id=job_id, server_alias=server_alias)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_preview(project_id: str, script_path: str, gpu_selection: GpuSelection | None = None) -> Any:
        """Preview a remote batch script without submitting a job."""
        try:
            kwargs = {} if gpu_selection is None else {'gpu_selection':gpu_selection}
            return await container.slurm_preview.preview(project_id=project_id, script_path=script_path, **kwargs)
        except HpcMcpError as error:
            return ToolFailure.from_error(error)

    @mcp.tool
    async def slurm_submit(project_id: str, script_path: str, request_id: UUID, gpu_selection: GpuSelection | None = None) -> Any:
        """Submit a validated batch script idempotently; reuse the request UUID on retries."""
        try:
            kwargs = {} if gpu_selection is None else {'gpu_selection':gpu_selection}
            return await container.slurm_submit.submit(project_id=project_id, script_path=script_path, request_id=request_id, **kwargs)
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
