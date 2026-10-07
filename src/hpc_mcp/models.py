from typing import Any, Literal

from pydantic import BaseModel

from hpc_mcp.errors import HpcMcpError


class ErrorBody(BaseModel):
    code: str
    message: str
    retriable: bool
    details: dict[str, Any]


class ToolFailure(BaseModel):
    ok: Literal[False] = False
    error: ErrorBody

    @classmethod
    def from_error(cls, error: HpcMcpError) -> "ToolFailure":
        return cls(
            error=ErrorBody(
                code=error.code,
                message=error.message,
                retriable=error.retriable,
                details=error.details,
            )
        )
