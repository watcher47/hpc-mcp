from pathlib import Path

from hpc_mcp.server import parse_config_file


def test_mcp_cli_accepts_config_file_override(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"

    assert parse_config_file(["--config-file", str(path)]) == path.resolve()


def test_mcp_cli_uses_environment_override(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"

    assert parse_config_file([], {"HPC_MCP_CONFIG": str(path)}) == path.resolve()
