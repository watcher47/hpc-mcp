from typing import Any


class HpcMcpError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        retriable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retriable = retriable
        self.details = details or {}
