"""STDIO entry point; importing this module does not build services or run a server."""

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from fastmcp import FastMCP

from hpc_mcp.config.paths import resolve_servers_path
from hpc_mcp.container import ServiceContainer, build_container
from hpc_mcp.tools.connection import register_connection_tools
from hpc_mcp.tools.project import register_project_tools
from hpc_mcp.tools.remote import register_remote_tools
from hpc_mcp.tools.slurm import register_slurm_tools
from hpc_mcp.tools.ssh import register_ssh_tools


def create_server(container: ServiceContainer) -> FastMCP:
    mcp = FastMCP("HPC SSH and Slurm")
    register_connection_tools(mcp, container)
    register_project_tools(mcp, container)
    register_remote_tools(mcp, container)
    register_slurm_tools(mcp, container)
    register_ssh_tools(mcp, container)
    return mcp


def parse_config_file(
    argv: Sequence[str], environ: Mapping[str, str] | None = None
) -> Path:
    parser = argparse.ArgumentParser(prog="hpc-mcp")
    parser.add_argument("--config-file")
    options = parser.parse_args(list(argv))
    return resolve_servers_path(
        options.config_file, os.environ if environ is None else environ
    )


def main(argv: Sequence[str] | None = None) -> None:
    config_file = parse_config_file(sys.argv[1:] if argv is None else argv)
    create_server(build_container(config_file=config_file)).run(transport="stdio")


if __name__ == "__main__":
    main()
