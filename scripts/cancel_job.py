"""Call the managed MCP's native owner-checked cancellation tool once.

Keep the same --request-id for recovery. This client never calls SSH/scancel directly.
Run with a Python environment containing the installed hpc-codex-mcp package.
"""
import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from fastmcp import Client
from fastmcp.client.transports import StdioTransport

from hpc_mcp.setup.install import Installer


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--request-id", type=UUID, required=True)
    parser.add_argument("--server", default=None)
    args = parser.parse_args()
    metadata = json.loads((Path.home() / ".hpc-codex-mcp/state/installation.json").read_text(encoding="utf-8"))
    record = Installer(Path(metadata["config"]))._load()
    if record is None:
        raise RuntimeError("Managed installation is absent")
    command = record["managed_command"]
    async with Client(StdioTransport(command=command[0], args=command[1:])) as client:
        result = await client.call_tool("slurm_cancel_job", {
            "job_id": args.job_id, "request_id": str(args.request_id), "server_alias": args.server,
        })
        value = result.data
        print(json.dumps(value, indent=2, ensure_ascii=False))
        if result.is_error or (isinstance(value, dict) and value.get("ok") is False):
            raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
