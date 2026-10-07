from hpc_mcp.errors import HpcMcpError
from hpc_mcp.models import ToolFailure


def test_tool_failure_does_not_expose_exception_repr() -> None:
    error = HpcMcpError(
        code="AUTH_FAILED",
        message="SSH authentication failed",
        retriable=False,
        details={"host": "gpu.internal"},
    )

    result = ToolFailure.from_error(error)

    assert result.model_dump() == {
        "ok": False,
        "error": {
            "code": "AUTH_FAILED",
            "message": "SSH authentication failed",
            "retriable": False,
            "details": {"host": "gpu.internal"},
        },
    }
