from __future__ import annotations

import hashlib
import stat as stat_module
from dataclasses import dataclass
from typing import Any

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import RemotePathGuard
from hpc_mcp.ssh.protocol import SshClientProtocol
from hpc_mcp.remote.files import (
    _is_kind,
    _maybe_await,
    _project_record,
    _read_remote_bytes,
    _redact_io_errors,
    _reject_remote_symlink_components,
    _write_remote_bytes,
)


@dataclass(frozen=True, slots=True)
class TextEdit:
    old_text: str
    new_text: str
    expected_count: int = 1

    def __post_init__(self) -> None:
        if not self.old_text:
            raise ValueError("old_text must not be empty")
        if self.expected_count < 1:
            raise ValueError("expected_count must be positive")


@dataclass(frozen=True, slots=True)
class PatchResult:
    relative_path: str
    old_sha256: str
    new_sha256: str
    changed_bytes: int

    @property
    def changed_byte_count(self) -> int:
        return self.changed_bytes


class RemotePatchService:
    def __init__(self, registry: Any, client: SshClientProtocol) -> None:
        self._registry = registry
        self._client = client

    @_redact_io_errors
    async def apply(
        self,
        project_id: str,
        relative_path: str,
        expected_sha256: str,
        edits: list[TextEdit],
    ) -> PatchResult:
        record = await _project_record(self._registry, project_id)
        target = await RemotePathGuard.resolve(self._client, record, relative_path)
        remote_root = await RemotePathGuard.resolve(self._client, record, ".")

        async with self._client.sftp() as sftp:
            await _reject_remote_symlink_components(sftp, remote_root, relative_path)
            attrs = await _maybe_await(sftp.lstat(target))
            if not _is_kind(attrs, stat_module.S_ISREG):
                raise HpcMcpError("REMOTE_NOT_FILE", "Remote path is not a regular file")
            original = await _read_remote_bytes(sftp, target)
            old_sha256 = hashlib.sha256(original).hexdigest()
            if old_sha256 != expected_sha256:
                raise HpcMcpError(
                    "REMOTE_CHANGED",
                    "Remote file changed since it was read",
                    details={
                        "expected_sha256": expected_sha256,
                        "actual_sha256": old_sha256,
                    },
                )
            try:
                updated_text = original.decode("utf-8")
            except UnicodeDecodeError as error:
                raise HpcMcpError(
                    "REMOTE_DECODE_ERROR",
                    "Remote file is not valid UTF-8",
                    details={"offset": error.start},
                ) from None

            for index, edit in enumerate(edits):
                actual_count = updated_text.count(edit.old_text)
                if actual_count != edit.expected_count:
                    raise HpcMcpError(
                        "EDIT_COUNT_MISMATCH",
                        "Text edit matched an unexpected number of occurrences",
                        details={
                            "edit_index": index,
                            "expected": edit.expected_count,
                            "actual": actual_count,
                        },
                    )
                updated_text = updated_text.replace(
                    edit.old_text, edit.new_text, edit.expected_count
                )

            updated = updated_text.encode("utf-8")
            new_sha256 = hashlib.sha256(updated).hexdigest()
            changed_bytes = _changed_byte_count(original, updated)
            if updated != original:
                permissions = int(getattr(attrs, "permissions", 0) or 0)
                await _write_remote_bytes(
                    sftp,
                    target,
                    updated,
                    stat_module.S_IMODE(permissions),
                    temporary_tag="patch",
                )

        return PatchResult(
            relative_path=relative_path,
            old_sha256=old_sha256,
            new_sha256=new_sha256,
            changed_bytes=changed_bytes,
        )


def _changed_byte_count(before: bytes, after: bytes) -> int:
    """Count the changed span after a common prefix/suffix in O(n) time.

    This is a conservative span size, not a minimum edit distance: unchanged
    bytes between separate edits are included. Memory overhead is constant.
    """
    if before == after:
        return 0
    shared = min(len(before), len(after))
    prefix = 0
    while prefix < shared and before[prefix] == after[prefix]:
        prefix += 1
    suffix = 0
    while suffix < shared - prefix and before[-suffix - 1] == after[-suffix - 1]:
        suffix += 1
    return max(len(before), len(after)) - prefix - suffix
