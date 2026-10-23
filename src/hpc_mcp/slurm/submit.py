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
from hpc_mcp.slurm.preview import read_remote_script, create_submission_preview
from hpc_mcp.slurm.gpu import GpuSelection
from hpc_mcp.slurm.gpu_submission import script_header
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
    version: int | None = None
    gpu_selection: dict | None = None
    gpu_plan: dict | None = None
    source_sha256: str | None = None
    submission_script: str | None = None
    expected_plan: str | None = None
    preflight_snapshot: str | None = None
    guard_report: str | None = None
    stdout_template: str | None = None
    stderr_template: str | None = None


def _unknown() -> HpcMcpError:
    return HpcMcpError('SUBMISSION_UNKNOWN', 'Slurm submission outcome is unknown',
                       details={'recovery': 'Reconcile this request ID before submitting a new request.'})


class SlurmSubmitService:
    def __init__(self, registry: Any, client: SshClientProtocol, remote_files: Any = None) -> None:
        self._registry = registry
        self._client = client
        self._files = remote_files if remote_files is not None else RemoteFileService(registry, client)

    async def submit(self, project_id: str, script_path: str, request_id: UUID, gpu_selection: GpuSelection | None = None) -> SubmissionReceipt:
        request = str(UUID(str(request_id)))
        selector = None if gpu_selection is None else GpuSelection.model_validate(gpu_selection).model_dump(exclude_none=True)
        record = await _project_record(self._registry, project_id)
        root = await RemotePathGuard.resolve(self._client, record, ".")
        script = await RemotePathGuard.resolve(self._client, record, script_path)
        relative_script = posixpath.relpath(script, root)
        # Accepted GPU receipts are definitive. Lookup precedes local script
        # semantics and any deployment/probe, so a replay never chooses anew.
        existing = await self._recover(root, request, relative_script, missing_metadata_ok=True)
        source_digest = None
        if existing is not None and (selector is not None or existing.version == 2):
            if existing.gpu_selection != selector:
                raise HpcMcpError('SUBMISSION_CONFLICT', 'Request UUID has a different GPU selection')
            source = await read_remote_script(self._files, project_id, script_path)
            source_digest = hashlib.sha256(source.encode()).hexdigest()
            self._check_intent(existing, selector, source_digest)
            return existing
        if selector is None:
            await preview_remote_submission(self._files, project_id, script_path)
        else:
            source = await read_remote_script(self._files, project_id, script_path)
            try:
                script_header(source)
                create_submission_preview(script_path, source)
            except ValueError as error:
                raise HpcMcpError('GPU_SCRIPT_INVALID', str(error)) from None
            source_digest = hashlib.sha256(source.encode()).hexdigest()
        helper_path = await self._deploy(root)
        args = ['-I', helper_path, '--project-root', root, '--script',
                './' + relative_script if relative_script.startswith('-') else relative_script,
                '--request-id', request]
        if selector is not None:
            probe_path, probe_digest = await self._deploy_gpu_module(root, 'resource_probe')
            rules_path, rules_digest = await self._deploy_gpu_module(root, 'gpu_submission')
            args.extend(['--gpu-selection-json', json.dumps(selector, sort_keys=True), '--gpu-probe', probe_path, '--gpu-probe-sha256', probe_digest, '--gpu-helper', rules_path, '--gpu-helper-sha256', rules_digest])
        command = RemoteCommand(
            'python3', args, cwd=root, timeout_seconds=600,
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
                        receipt = self._receipt(result.stdout, request, relative_script)
                        self._check_intent(receipt, selector, source_digest)
                        return receipt
                    except (ValueError, TypeError):
                        pass
                elif result.exit_status in {4, 75, 76, 3} and selector is not None and not result.truncated:
                    try:
                        error_data = json.loads(result.stdout)
                    except (ValueError, TypeError):
                        error_data = {}
                    expected_code = {4:'SUBMISSION_CONFLICT', 75:'GPU_BUSY', 76:'GPU_UNKNOWN'}.get(result.exit_status)
                    if expected_code and error_data == {'error':expected_code}:
                        raise HpcMcpError(expected_code, 'Request UUID has a different submission intent' if expected_code == 'SUBMISSION_CONFLICT' else 'GPU selection is busy or cannot be verified; inspect slurm_gpu_inventory')
                    if result.exit_status in {3,75,76} and isinstance(error_data, dict) and error_data.get('error') in {'GPU_INVALID','GPU_BUSY','GPU_UNKNOWN'} and set(error_data) == {'error','message'} and isinstance(error_data['message'], str):
                        message = error_data['message'] if error_data['error'] == 'GPU_INVALID' else 'GPU selection is busy or cannot be verified; inspect slurm_gpu_inventory'
                        raise HpcMcpError('GPU_SCRIPT_INVALID' if error_data['error'] == 'GPU_INVALID' else error_data['error'], message)
                elif result.exit_status in {1, 3, 4} and not result.truncated:
                    expected = 'SUBMISSION_FAILED' if result.exit_status == 1 else 'SUBMISSION_INVALID'
                    if result.exit_status == 4:
                        expected = 'SUBMISSION_CONFLICT'
                    try:
                        confirmed = json.loads(result.stdout) == {'error': expected}
                    except (ValueError, TypeError):
                        confirmed = False
                    if confirmed:
                        raise HpcMcpError(expected, 'Slurm submission failed' if result.exit_status == 1 else 'Slurm submission request is invalid')
                # A killed helper or malformed output may already have submitted.
                receipt = await self._recover(root, request, relative_script)
                if receipt is not None:
                    self._check_intent(receipt, selector, source_digest)
                    return receipt
                raise _unknown()

            # SshClient opens a fresh connection for each SFTP operation. A failed
            # recovery read forbids retry; only a confirmed absent receipt permits it.
            receipt = await self._recover(root, request, relative_script)
            if receipt is not None:
                self._check_intent(receipt, selector, source_digest)
                return receipt
            if attempt == 1:
                raise _unknown()
        raise _unknown()

    @staticmethod
    def _receipt(raw: str | bytes, request: str, script: str) -> SubmissionReceipt:
        if len(raw) > RECEIPT_LIMIT:
            raise ValueError('Invalid submission receipt')
        return SubmissionReceipt(**validate_receipt(json.loads(raw), request, script))

    @staticmethod
    def _check_intent(receipt, selector, source_digest):
        if receipt.version == 2 or selector is not None:
            if receipt.gpu_selection != selector or receipt.source_sha256 != source_digest:
                raise HpcMcpError('SUBMISSION_CONFLICT', 'Request UUID has a different selector or script contents')

    async def _recover(self, root: str, request: str, script: str, *, missing_metadata_ok=False) -> SubmissionReceipt | None:
        try:
            async with self._client.sftp() as sftp:
                await self._metadata(sftp, root, 'submissions', create=False)
                raw = await self._read(sftp, posixpath.join(root, '.hpc/submissions', f'{request}.json'), RECEIPT_LIMIT)
                return None if raw is None else self._receipt(raw, request, script)
        except (FileNotFoundError, SFTPNoSuchFile):
            if missing_metadata_ok:
                return None
            raise _unknown() from None
        except (HpcMcpError, OSError, SFTPError, ValueError, TypeError):
            raise _unknown() from None

    async def _deploy_gpu_module(self, root: str, name: str) -> tuple[str, str]:
        source = Path(__file__).with_name(name+'.py').read_bytes()
        digest = hashlib.sha256(source).hexdigest()
        target = posixpath.join(root, '.hpc/bin', name+'.'+digest+'.py')
        try:
            async with self._client.sftp() as sftp:
                await self._metadata(sftp, root, 'bin', create=True)
                remote = await self._read(sftp, target, len(source))
                if remote is None:
                    await _write_remote_bytes(sftp, target, source, 0o700, 'gpu-helper')
                elif remote != source:
                    raise ValueError('Immutable GPU helper differs')
                if await self._read(sftp, target, len(source)) != source:
                    raise ValueError('GPU helper verification failed')
            return target, digest
        except HpcMcpError:
            raise
        except (OSError, SFTPError, ValueError):
            raise HpcMcpError('HELPER_DEPLOYMENT_FAILED', 'GPU helper deployment failed') from None

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
