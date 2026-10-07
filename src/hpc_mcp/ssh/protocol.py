from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from hpc_mcp.ssh.policy import RemoteCommand


class CommandResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    exit_status: int
    stdout: str
    stderr: str
    truncated: bool = False


class SshClientProtocol(Protocol):
    async def run(self, command: RemoteCommand) -> CommandResult: ...

    async def run_general(self, cmd_string: str, directory: str,
                          timeout_ms: int | None) -> CommandResult: ...

    def sftp(self) -> AbstractAsyncContextManager[Any]: ...

    async def realpath(self, path: str) -> str: ...
