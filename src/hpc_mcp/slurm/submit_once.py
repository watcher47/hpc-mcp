"""Standalone, standard-library-only helper deployed to the remote project.

Receipts and locks must be retained. A pending marker without a receipt means
the job may have been accepted; it requires reconciliation, never resubmission.
"""
from __future__ import annotations

import argparse
import errno
import json
import os
import re
import stat
import subprocess
import time
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4


JOB_ID = re.compile(r'^[0-9]+(?:_[0-9]+)?$')
RECEIPT_LIMIT = 16384


class SubmissionUnknown(Exception):
    pass


class SubmissionFailed(Exception):
    pass


class SubmissionInvalid(ValueError):
    """A request rejected before examining or changing submission state."""


def validate_receipt(value: object, request_id: str, script: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {'request_id', 'job_id', 'script', 'submitted_at'}:
        raise ValueError('Invalid submission receipt')
    if not all(isinstance(item, str) for item in value.values()):
        raise ValueError('Invalid submission receipt')
    if value['request_id'] != request_id or value['script'] != script:
        raise ValueError('Submission receipt does not match request')
    if JOB_ID.fullmatch(value['job_id']) is None:
        raise ValueError('Invalid submission receipt')
    stamp = datetime.fromisoformat(value['submitted_at'].replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Invalid submission receipt')
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


def submit_once(project_root: str | Path, script: str, request_id: UUID | str) -> dict[str, str]:
    try:
        request = str(UUID(str(request_id)))
        root = Path(project_root).resolve(strict=True)
        _, canonical_script = _script(root, script)
    except (ValueError, OSError):
        raise SubmissionInvalid('Slurm submission request is invalid') from None
    try:
        return _submit_validated(root, script, request, canonical_script)
    except (SubmissionUnknown, SubmissionFailed):
        raise
    except Exception:
        # Persisted state may represent an earlier accepted job, and decoding or
        # processing may fail after dispatch. Never classify these as invalid
        # requests or remove the pending marker: the caller must reconcile.
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None


def _submit_validated(root: Path, script: str, request: str, canonical_script: str) -> dict[str, str]:
    if os.name == 'nt':
        return _submit_validated_windows(root, script, request, canonical_script)

    with ExitStack() as stack:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, root_fd)
        metadata = _directory(stack, '.hpc', root_fd)
        directory = _directory(stack, 'submissions', metadata)
        lock_fd = _open_regular(f'{request}.lock', os.O_RDWR | os.O_CREAT, directory)
        stack.callback(os.close, lock_fd)
        with _exclusive_lock(lock_fd):
            return _submit_locked(root, script, request, canonical_script, directory)


def _submit_validated_windows(
    root: Path, script: str, request: str, canonical_script: str
) -> dict[str, str]:
    metadata = _path_directory(root, '.hpc')
    directory = _path_directory(metadata, 'submissions')
    lock_fd = _open_regular(f'{request}.lock', os.O_RDWR | os.O_CREAT, directory)
    try:
        with _exclusive_lock(lock_fd):
            return _submit_locked(root, script, request, canonical_script, directory)
    finally:
        os.close(lock_fd)


def _submit_locked(
    root: Path,
    script: str,
    request: str,
    canonical_script: str,
    directory: int | Path,
) -> dict[str, str]:
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
        return validate_receipt(json.loads(data), request, canonical_script)

    pending = f'{request}.pending'
    try:
        pending_fd = _open_regular(
            pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL, directory
        )
    except FileExistsError:
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None
    with os.fdopen(pending_fd, 'w', encoding='utf-8') as stream:
        stream.write(canonical_script)
        stream.flush()
        os.fsync(stream.fileno())
    if not isinstance(directory, Path):
        os.fsync(directory)
    resolved, checked_script = _script(root, script)
    if checked_script != canonical_script:
        raise ValueError('Project script changed during submission')
    try:
        result = subprocess.run(
            ['sbatch', '--parsable', str(resolved)],
            cwd=str(root), capture_output=True, text=True,
        )
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
    try:
        _atomic_receipt(directory, receipt_name, receipt)
    except OSError:
        raise SubmissionUnknown('Slurm submission outcome is unknown') from None
    # Retaining pending is intentional: even accidental receipt deletion
    # must not permit a second submission with this request ID.
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--project-root', required=True)
    parser.add_argument('--script', required=True)
    parser.add_argument('--request-id', required=True, type=UUID)
    args = parser.parse_args(argv)
    try:
        receipt = submit_once(args.project_root, args.script, args.request_id)
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
