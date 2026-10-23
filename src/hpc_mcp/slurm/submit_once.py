"""Standalone, standard-library-only helper deployed to the remote project.

Receipts and locks must be retained. A pending marker without a receipt means
the job may have been accepted; it requires reconciliation, never resubmission.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import importlib.util
import json
import os
import re
import stat
import subprocess
import time
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


JOB_ID = re.compile(r'^[0-9]+(?:_[0-9]+)?$')
RECEIPT_LIMIT = 16384


class SubmissionUnknown(Exception):
    pass


class SubmissionFailed(Exception):
    pass


class SubmissionInvalid(ValueError):
    """A request rejected before examining or changing submission state."""


class SubmissionConflict(Exception):
    """A known UUID is being reused for a different submission intent."""


class GpuRejected(Exception):
    def __init__(self, status: str, message: str):
        self.status = status
        super().__init__(message)


_GPU_RULES = None


def _rules():
    if _GPU_RULES is not None:
        return _GPU_RULES
    from hpc_mcp.slurm import gpu_submission
    return gpu_submission


def _load_verified(root: Path, path: str, digest: str):
    target = Path(path)
    if not re.fullmatch(r'[0-9a-f]{64}', digest) or target.is_symlink() or not target.resolve(strict=True).is_relative_to(root / '.hpc' / 'bin'):
        raise ValueError('Invalid GPU helper path')
    if not stat.S_ISREG(target.stat().st_mode) or target.stat().st_nlink != 1:
        raise ValueError('GPU helper must be a regular file')
    data = target.read_bytes()
    if len(data) > 1024 * 1024 or hashlib.sha256(data).hexdigest() != digest:
        raise ValueError('GPU helper verification failed')
    spec = importlib.util.spec_from_file_location('_hpc_' + target.stem, target)
    module = importlib.util.module_from_spec(spec)
    exec(compile(data, str(target), 'exec'), module.__dict__)
    return module


def validate_receipt(value: object, request_id: str, script: str) -> dict[str, Any]:
    base = {'request_id', 'job_id', 'script', 'submitted_at'}
    gpu_fields = {'version', 'gpu_selection', 'gpu_plan', 'source_sha256', 'submission_script', 'expected_plan', 'preflight_snapshot', 'guard_report', 'stdout_template', 'stderr_template'}
    if not isinstance(value, dict) or set(value) not in (base, base | gpu_fields):
        raise ValueError('Invalid submission receipt')
    if not all(isinstance(value[key], str) for key in base):
        raise ValueError('Invalid submission receipt')
    if value['request_id'] != request_id or value['script'] != script:
        raise ValueError('Submission receipt does not match request')
    if JOB_ID.fullmatch(value['job_id']) is None:
        raise ValueError('Invalid submission receipt')
    stamp = datetime.fromisoformat(value['submitted_at'].replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Invalid submission receipt')
    if 'version' in value:
        rules = _rules()
        if type(value['version']) is not int or value['version'] != 2 or rules.canonical_selector(value['gpu_selection']) != value['gpu_selection']:
            raise ValueError('Invalid GPU receipt version or selector')
        rules.validate_plan(value['gpu_plan'])
        if not isinstance(value['source_sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', value['source_sha256']):
            raise ValueError('Invalid source digest')
        for field, suffix in (('submission_script', '.sh'), ('expected_plan', '.expected.json'), ('preflight_snapshot', '.preflight.json'), ('guard_report', '.guard-${SLURM_JOB_ID}.json')):
            if value[field] != f'.hpc/submissions/{request_id}{suffix}':
                raise ValueError('Invalid GPU diagnostic path')
        for field in ('stdout_template', 'stderr_template'):
            template = value[field]
            if template is not None and (not isinstance(template, str) or not template or Path(template).is_absolute() or template.startswith('/') or '..' in template.split('/') or any(c in template for c in '\\\x00\r\n')):
                raise ValueError('Invalid GPU log template')
    return value


def _script(root: Path, script: str) -> tuple[Path, str]:
    requested = Path(script)
    if not script or any(c in script for c in '\x00\n\r') or requested.is_absolute() or '..' in requested.parts:
        raise ValueError('Invalid project script path')
    resolved = (root / requested).resolve(strict=True)
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise ValueError('Script is outside project or is not a regular file')
    return resolved, resolved.relative_to(root).as_posix()


def _directory(stack: ExitStack, name: str, parent: int) -> int:
    try:
        os.mkdir(name, 0o700, dir_fd=parent)
    except FileExistsError:
        pass
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    stack.callback(os.close, fd)
    # Include directories initially created by the SFTP deployment path.
    os.fsync(parent)
    return fd


def _open_regular(name: str, flags: int, directory: int | Path) -> int:
    if isinstance(directory, Path):
        path = directory / name
        if path.is_symlink():
            raise ValueError('Submission metadata must be a regular file')
        fd = os.open(
            path,
            flags | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOINHERIT', 0),
            0o600,
        )
    else:
        fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
    attrs = os.fstat(fd)
    if not stat.S_ISREG(attrs.st_mode) or attrs.st_nlink != 1:
        os.close(fd)
        raise ValueError('Submission metadata must be a regular file')
    return fd


def _atomic_receipt(directory: int | Path, name: str, receipt: dict[str, str]) -> None:
    temporary = f'.{name}.{uuid4().hex}.tmp'
    fd = _open_regular(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, directory)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        if isinstance(directory, Path):
            os.replace(directory / temporary, directory / name)
        else:
            os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
    finally:
        try:
            if isinstance(directory, Path):
                os.unlink(directory / temporary)
            else:
                os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass


def _is_reparse_point(path: Path) -> bool:
    attrs = os.lstat(path)
    attributes = getattr(attrs, 'st_file_attributes', 0)
    reparse_flag = getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400)
    return bool(attributes & reparse_flag)


def _path_directory(parent: Path, name: str) -> Path:
    directory = parent / name
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        pass
    if (
        _is_reparse_point(directory)
        or directory.is_symlink()
        or not directory.is_dir()
    ):
        raise ValueError('Submission metadata must be a real directory')
    return directory


@contextmanager
def _exclusive_lock(fd: int):
    if os.name != 'nt':
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
        return

    # This branch supports local Windows validation. The deployed Slurm helper
    # keeps using flock and descriptor-relative, no-follow operations on POSIX.
    import msvcrt

    while True:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            break
        except OSError as error:
            if error.errno not in {errno.EACCES, errno.EDEADLK}:
                raise
            time.sleep(0.01)
    try:
        # Windows byte-range locks can cover the first byte of an empty file.
        # Initialize that byte only after holding the lock: a second process
        # must not write through the first process's mandatory byte lock.
        if os.fstat(fd).st_size == 0:
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, b'\0')
            os.fsync(fd)
        yield
    finally:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


def submit_once(project_root: str | Path, script: str, request_id: UUID | str, *, gpu_selection=None, gpu_probe=None) -> dict[str, Any]:
    try:
        request = str(UUID(str(request_id)))
        root = Path(project_root).resolve(strict=True)
        _, canonical_script = _script(root, script)
        if gpu_selection is not None:
            gpu_selection = _rules().canonical_selector(gpu_selection)
    except (ValueError, OSError):
        raise SubmissionInvalid('Slurm submission request is invalid') from None
    try:
        return _submit_validated(root, script, request, canonical_script, gpu_selection, gpu_probe)
    except (SubmissionUnknown, SubmissionFailed, SubmissionConflict, GpuRejected):
        raise
    except Exception:
        # Persisted state may represent an earlier accepted job, and decoding or
        # processing may fail after dispatch. Never classify these as invalid
        # requests or remove the pending marker: the caller must reconcile.
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None


def _submit_validated(root: Path, script: str, request: str, canonical_script: str, gpu_selection=None, gpu_probe=None) -> dict[str, Any]:
    if os.name == 'nt':
        return _submit_validated_windows(root, script, request, canonical_script, gpu_selection, gpu_probe)

    with ExitStack() as stack:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, root_fd)
        metadata = _directory(stack, '.hpc', root_fd)
        directory = _directory(stack, 'submissions', metadata)
        lock_fd = _open_regular(f'{request}.lock', os.O_RDWR | os.O_CREAT, directory)
        stack.callback(os.close, lock_fd)
        with _exclusive_lock(lock_fd):
            return _submit_locked(root, script, request, canonical_script, directory, gpu_selection, gpu_probe)


def _submit_validated_windows(
    root: Path, script: str, request: str, canonical_script: str, gpu_selection=None, gpu_probe=None
) -> dict[str, Any]:
    metadata = _path_directory(root, '.hpc')
    directory = _path_directory(metadata, 'submissions')
    lock_fd = _open_regular(f'{request}.lock', os.O_RDWR | os.O_CREAT, directory)
    try:
        with _exclusive_lock(lock_fd):
            return _submit_locked(root, script, request, canonical_script, directory, gpu_selection, gpu_probe)
    finally:
        os.close(lock_fd)


def _submit_locked(
    root: Path,
    script: str,
    request: str,
    canonical_script: str,
    directory: int | Path,
    gpu_selection=None,
    gpu_probe=None,
) -> dict[str, Any]:
    receipt_name = f'{request}.json'
    try:
        receipt_fd = _open_regular(receipt_name, os.O_RDONLY, directory)
    except FileNotFoundError:
        pass
    else:
        with os.fdopen(receipt_fd, 'rb') as stream:
            data = stream.read(RECEIPT_LIMIT + 1)
        if len(data) > RECEIPT_LIMIT:
            raise ValueError('Invalid submission receipt')
        decoded = json.loads(data)
        receipt = validate_receipt(decoded, request, decoded.get('script') if isinstance(decoded, dict) else canonical_script)
        if gpu_selection is not None or 'version' in receipt:
            if receipt['script'] != canonical_script or receipt.get('gpu_selection') != gpu_selection:
                raise SubmissionConflict('Request UUID has a different GPU selection')
            if _source(root, script)[1] != receipt['source_sha256']:
                raise SubmissionConflict('Request UUID has different script contents')
        elif receipt['script'] != canonical_script:
            raise ValueError('Submission receipt does not match script')
        return receipt

    # Check uncertain state before any fresh selection, including CPU/GPU changes.
    try:
        existing_pending = _open_regular(f'{request}.pending', os.O_RDONLY, directory)
    except FileNotFoundError:
        pass
    else:
        os.close(existing_pending)
        raise SubmissionUnknown('Slurm submission outcome is unknown')

    gpu_intent = None
    if gpu_selection is not None:
        gpu_intent = _prepare_gpu(root, script, request, canonical_script, directory, gpu_selection, gpu_probe)
    else:
        try:
            intent_fd = _open_regular(f'{request}.intent.json', os.O_RDONLY, directory)
        except FileNotFoundError:
            pass
        else:
            os.close(intent_fd)
            raise SubmissionConflict('Request UUID has a GPU submission intent')

    pending = f'{request}.pending'
    try:
        pending_fd = _open_regular(
            pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL, directory
        )
    except FileExistsError:
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None
    with os.fdopen(pending_fd, 'w', encoding='utf-8') as stream:
        stream.write(json.dumps(gpu_intent, sort_keys=True) if gpu_intent is not None else canonical_script)
        stream.flush()
        os.fsync(stream.fileno())
    if not isinstance(directory, Path):
        os.fsync(directory)
    resolved, checked_script = _script(root, script)
    if checked_script != canonical_script:
        raise ValueError('Project script changed during submission')
    try:
        argv = ['sbatch', '--parsable', str(resolved)]
        kwargs = {'cwd':str(root), 'capture_output':True, 'text':True}
        if gpu_intent is not None:
            plan = gpu_intent['gpu_plan']
            argv = ['sbatch', '--parsable', '--nodelist='+plan['node'], '--nodes=1', '--gres='+plan['gres'], str(root / gpu_intent['submission_script'])]
            kwargs['env'] = {key:value for key, value in os.environ.items() if not key.startswith('SBATCH_')}
        result = subprocess.run(argv, **kwargs)
    except OSError:
        # An OSError may occur after sbatch accepted the job. Retain the
        # pending marker so retries cannot dispatch the request again.
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None
    if result.returncode > 0:
        if isinstance(directory, Path):
            os.unlink(directory / pending)
        else:
            os.unlink(pending, dir_fd=directory)
            os.fsync(directory)
        raise SubmissionFailed('Slurm submission failed')
    job_id = result.stdout.removesuffix('\n')
    if result.returncode != 0 or JOB_ID.fullmatch(job_id) is None:
        raise SubmissionUnknown('Slurm submission outcome is unknown')
    receipt = {
        'request_id': request, 'job_id': job_id, 'script': canonical_script,
        'submitted_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
    }
    if gpu_intent is not None:
        receipt.update({key:value for key,value in gpu_intent.items() if key not in {'request_id', 'script', 'submitted_at'}})
    try:
        _atomic_receipt(directory, receipt_name, receipt)
    except OSError:
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None
    # Retaining pending is intentional: even accidental receipt deletion
    # must not permit a second submission with this request ID.
    return receipt


def _source(root, script):
    path, _ = _script(root, script)
    with path.open('rb') as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise GpuRejected('invalid', 'Batch script exceeds the 1 MiB limit')
    return data.decode('utf-8'), hashlib.sha256(data).hexdigest()


def _immutable(directory, name, data):
    try:
        fd = _open_regular(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, directory)
    except FileExistsError:
        fd = _open_regular(name, os.O_RDONLY, directory)
        with os.fdopen(fd, 'rb') as stream:
            if stream.read(len(data) + 1) != data:
                raise SubmissionUnknown('Immutable submission snapshot differs')
        return
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    if not isinstance(directory, Path):
        os.fsync(directory)


def _prepare_gpu(root, script, request, canonical_script, directory, selector, probe):
    rules = _rules()
    source, digest = _source(root, script)
    try:
        intent_fd = _open_regular(f'{request}.intent.json', os.O_RDONLY, directory)
    except FileNotFoundError:
        intent = None
    else:
        with os.fdopen(intent_fd, 'rb') as stream:
            data = stream.read(RECEIPT_LIMIT + 1)
        if len(data) > RECEIPT_LIMIT:
            raise ValueError('Invalid GPU intent')
        intent = json.loads(data)
        validate_receipt(dict(intent, job_id='0'), request, intent['script'])
        if intent['script'] != canonical_script or intent['gpu_selection'] != selector or intent['source_sha256'] != digest:
            raise SubmissionConflict('Request UUID has a different submission intent')
        return intent
    try:
        _, _, options = rules.script_header(source)
        if probe is None:
            raise ValueError('Verified GPU probe is required')
        snapshot = probe.collect_snapshot()
        preflight = probe.evaluate_snapshot(snapshot, selector)
        status = preflight.get('status')
        if status not in {'ready', 'queueable'} or preflight.get('admission', {}).get('allowed') is not True:
            raise GpuRejected(status if status in {'busy', 'unknown'} else 'unknown', 'GPU selection is busy or cannot be verified; inspect slurm_gpu_inventory')
        plan = rules.validate_plan(preflight['selection'])
        rules.script_header(source, plan)
        probe_path = Path(probe.__file__)
        probe_digest = hashlib.sha256(probe_path.read_bytes()).hexdigest()
        if probe_path.is_symlink() or not probe_path.resolve(strict=True).is_relative_to(root / '.hpc' / 'bin'):
            raise ValueError('GPU probe must be deployed inside project metadata')
        base = f'.hpc/submissions/{request}'
        intent = dict(request_id=request, script=canonical_script, submitted_at=datetime.now(timezone.utc).isoformat(), version=2, gpu_selection=selector, gpu_plan=plan, source_sha256=digest, submission_script=base+'.sh', expected_plan=base+'.expected.json', preflight_snapshot=base+'.preflight.json', guard_report=base+'.guard-${SLURM_JOB_ID}.json', stdout_template=options.get('--output', 'slurm-%j.out'), stderr_template=options.get('--error'))
        validate_receipt(dict(intent, job_id='0'), request, canonical_script)
        script_text = rules.wrapper(source, root, probe_path, probe_digest, root / intent['expected_plan'], root / intent['guard_report'])
        raw_snapshot = json.dumps({'snapshot':snapshot, 'evaluation':preflight}, sort_keys=True).encode()
        if len(raw_snapshot) > 2 * 1024 * 1024:
            raise ValueError('GPU evidence exceeds size limit')
        if _source(root, script)[1] != digest:
            raise SubmissionConflict('Script changed during GPU preflight')
    except GpuRejected:
        raise
    except (ValueError, KeyError, TypeError, UnicodeError, OSError) as error:
        raise GpuRejected('invalid', str(error)) from None
    _immutable(directory, request+'.expected.json', json.dumps(plan, sort_keys=True).encode())
    _immutable(directory, request+'.preflight.json', raw_snapshot)
    _immutable(directory, request+'.sh', script_text.encode())
    _immutable(directory, request+'.intent.json', json.dumps(intent, sort_keys=True).encode())
    return intent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--project-root', required=True)
    parser.add_argument('--script', required=True)
    parser.add_argument('--request-id', required=True, type=UUID)
    parser.add_argument('--gpu-selection-json')
    parser.add_argument('--gpu-probe')
    parser.add_argument('--gpu-probe-sha256')
    parser.add_argument('--gpu-helper')
    parser.add_argument('--gpu-helper-sha256')
    args = parser.parse_args(argv)
    try:
        selection = None
        probe = None
        if args.gpu_selection_json is not None:
            global _GPU_RULES
            try:
                root = Path(args.project_root).resolve(strict=True)
                _GPU_RULES = _load_verified(root, args.gpu_helper, args.gpu_helper_sha256)
                probe = _load_verified(root, args.gpu_probe, args.gpu_probe_sha256)
                selection = json.loads(args.gpu_selection_json)
            except (ValueError, TypeError, OSError):
                raise SubmissionInvalid('GPU helper verification failed') from None
        receipt = submit_once(args.project_root, args.script, args.request_id, gpu_selection=selection, gpu_probe=probe)
    except SubmissionConflict:
        print(json.dumps({'error':'SUBMISSION_CONFLICT'}))
        return 4
    except GpuRejected as error:
        print(json.dumps({'error':'GPU_'+error.status.upper(), 'message':str(error)}))
        return 75 if error.status == 'busy' else 76 if error.status == 'unknown' else 3
    except SubmissionUnknown:
        print(json.dumps({'error': 'SUBMISSION_UNKNOWN'}))
        return 2
    except SubmissionFailed:
        print(json.dumps({'error': 'SUBMISSION_FAILED'}))
        return 1
    except SubmissionInvalid:
        print(json.dumps({'error': 'SUBMISSION_INVALID'}))
        return 3
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
