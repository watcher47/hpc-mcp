from __future__ import annotations

import hashlib
import json
import posixpath
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from asyncssh import SFTPError, SFTPNoSuchFile

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import RemotePathGuard
from hpc_mcp.remote.files import (
    RemoteFileService, _maybe_await, _project_record, _write_remote_bytes,
)
from hpc_mcp.slurm.preview import preview_remote_submission
from hpc_mcp.slurm.submit_once import RECEIPT_LIMIT, validate_receipt
from hpc_mcp.ssh.policy import RemoteCommand
from hpc_mcp.ssh.protocol import SshClientProtocol


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    request_id: str
    job_id: str
    script: str
    submitted_at: str
    stdout_path: str | None = None
    stderr_path: str | None = None
    watch_command: str | None = None


def _unknown() -> HpcMcpError:
    return HpcMcpError('SUBMISSION_UNKNOWN', 'Slurm submission outcome is unknown',
                       details={'recovery': 'Reconcile this request ID before submitting a new request.'})


class SlurmSubmitService:
    def __init__(self, registry: Any, client: SshClientProtocol, remote_files: Any = None) -> None:
        self._registry = registry
        self._client = client
        self._files = remote_files if remote_files is not None else RemoteFileService(registry, client)

    async def submit(self, project_id: str, script_path: str, request_id: UUID) -> SubmissionReceipt:
        request = str(UUID(str(request_id)))
        await preview_remote_submission(self._files, project_id, script_path)
        record = await _project_record(self._registry, project_id)
        root = await RemotePathGuard.resolve(self._client, record, ".")
        script = await RemotePathGuard.resolve(self._client, record, script_path)
        relative_script = posixpath.relpath(script, root)
        helper_path = await self._deploy(root)
        command = RemoteCommand(
            'python3', ['-I', helper_path, '--project-root', root, '--script',
                        './' + relative_script if relative_script.startswith('-') else relative_script,
                        '--request-id', request], cwd=root, timeout_seconds=600,
        )
        for attempt in range(2):
            try:
                result = await self._client.run(command)
            except HpcMcpError as error:
                if error.code not in {'SSH_TRANSPORT_FAILED', 'SSH_TIMEOUT'}:
                    raise
            except (OSError, TimeoutError):
                pass
            else:
                if result.exit_status == 0 and not result.truncated:
                    try:
                        return self._receipt(result.stdout, request, relative_script)
                    except (ValueError, TypeError):
                        pass
                elif result.exit_status in {1, 3} and not result.truncated:
                    expected = 'SUBMISSION_FAILED' if result.exit_status == 1 else 'SUBMISSION_INVALID'
                    try:
                        confirmed = json.loads(result.stdout) == {'error': expected}
                    except (ValueError, TypeError):
                        confirmed = False
                    if confirmed:
                        raise HpcMcpError(expected, 'Slurm submission failed' if result.exit_status == 1 else 'Slurm submission request is invalid')
                # A killed helper or malformed output may already have submitted.
                receipt = await self._recover(root, request, relative_script)
                if receipt is not None:
                    return receipt
                raise _unknown()

            # SshClient opens a fresh connection for each SFTP operation. A failed
            # recovery read forbids retry; only a confirmed absent receipt permits it.
            receipt = await self._recover(root, request, relative_script)
            if receipt is not None:
                return receipt
            if attempt == 1:
                raise _unknown()
        raise _unknown()

    @staticmethod
    def _receipt(raw: str | bytes, request: str, script: str) -> SubmissionReceipt:
        if len(raw) > RECEIPT_LIMIT:
            raise ValueError('Invalid submission receipt')
        return SubmissionReceipt(**validate_receipt(json.loads(raw), request, script))

    async def _recover(self, root: str, request: str, script: str) -> SubmissionReceipt | None:
        try:
            async with self._client.sftp() as sftp:
                await self._metadata(sftp, root, 'submissions', create=False)
                raw = await self._read(sftp, posixpath.join(root, '.hpc/submissions', f'{request}.json'), RECEIPT_LIMIT)
                return None if raw is None else self._receipt(raw, request, script)
        except (HpcMcpError, OSError, SFTPError, ValueError, TypeError):
            raise _unknown() from None

    async def _deploy(self, root: str) -> str:
        source = Path(__file__).with_name('submit_once.py').read_bytes()
        target = posixpath.join(root, '.hpc/bin/submit_once.py')
        try:
            async with self._client.sftp() as sftp:
                await self._metadata(sftp, root, 'bin', create=True)
                # A submissions directory also makes a missing receipt explicit.
                await self._metadata(sftp, root, 'submissions', create=True)
                try:
                    remote = await self._read(sftp, target, len(source))
                except ValueError:
                    # Larger content cannot match the packaged helper hash.
                    remote = None
                if remote is None or hashlib.sha256(remote).digest() != hashlib.sha256(source).digest():
                    await _write_remote_bytes(sftp, target, source, 0o700, 'submit-helper')
                deployed = await self._read(sftp, target, len(source))
                if deployed is None or hashlib.sha256(deployed).digest() != hashlib.sha256(source).digest():
                    raise HpcMcpError('HELPER_DEPLOYMENT_FAILED', 'Submission helper verification failed')
            return target
        except HpcMcpError:
            raise
        except (OSError, SFTPError, ValueError):
            raise HpcMcpError('HELPER_DEPLOYMENT_FAILED', 'Submission helper deployment failed') from None

    @staticmethod
    async def _metadata(sftp: Any, root: str, child: str, *, create: bool) -> None:
        for path in (posixpath.join(root, '.hpc'), posixpath.join(root, '.hpc', child)):
            try:
                attrs = await _maybe_await(sftp.lstat(path))
            except (FileNotFoundError, SFTPNoSuchFile):
                if not create:
                    raise
                await _maybe_await(sftp.makedirs(path, exist_ok=True))
                attrs = await _maybe_await(sftp.lstat(path))
            if not stat.S_ISDIR(getattr(attrs, 'permissions', 0) or 0):
                raise HpcMcpError('PATH_OUTSIDE_PROJECT', 'Submission metadata must be a real directory')

    @staticmethod
    async def _read(sftp: Any, path: str, limit: int) -> bytes | None:
        try:
            attrs = await _maybe_await(sftp.lstat(path))
        except (FileNotFoundError, SFTPNoSuchFile):
            return None
        if not stat.S_ISREG(getattr(attrs, 'permissions', 0) or 0):
            raise HpcMcpError('PATH_OUTSIDE_PROJECT', 'Submission metadata must be a regular file')
        if getattr(attrs, 'size', 0) > limit:
            raise ValueError('Submission metadata exceeds size limit')
        handle = await _maybe_await(sftp.open(path, 'rb'))
        try:
            data = await _maybe_await(handle.read(limit + 1))
        finally:
            await _maybe_await(handle.close())
        if len(data) > limit:
            raise ValueError('Submission metadata exceeds size limit')
        return data
