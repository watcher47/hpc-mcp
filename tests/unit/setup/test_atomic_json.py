import errno
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.setup import install


DELAYS = [0.01, 0.02, 0.04, 0.08, 0.16]


def filesystem_error(winerror=None, code=errno.EACCES):
    error = OSError(code, "fixture replacement failure")
    if winerror is not None:
        error.winerror = winerror
    return error


@pytest.fixture
def replacement_boundary(monkeypatch):
    # A module-local OS view avoids changing pathlib/pytest's host platform.
    local_os = SimpleNamespace(**vars(install.os))
    local_os.name = "nt"
    delays = []
    monkeypatch.setattr(install, "os", local_os)
    monkeypatch.setattr(install, "time", SimpleNamespace(sleep=delays.append), raising=False)
    return local_os, delays


@pytest.mark.parametrize("winerror", [5, 32, None])
@pytest.mark.parametrize("failures", [1, 3, 5])
def test_atomic_json_retries_only_replace_with_same_fsynced_temp(
    tmp_path, replacement_boundary, winerror, failures
):
    local_os, delays = replacement_boundary
    path = tmp_path / "pending-installation.json"
    path.write_bytes(b"old journal")
    value = {"phase": "registering"}
    replace, fsync = local_os.replace, local_os.fsync
    attempts, syncs = [], []

    def sync(descriptor):
        syncs.append(descriptor)
        fsync(descriptor)

    def fail_then_replace(source, target):
        attempts.append(source)
        assert len(syncs) == 1
        assert json.loads(Path(source).read_text()) == value
        assert target.read_bytes() == b"old journal"
        if len(attempts) <= failures:
            raise filesystem_error(winerror)
        replace(source, target)

    local_os.fsync = sync
    local_os.replace = fail_then_replace
    install._atomic_json(path, value)

    assert json.loads(path.read_text()) == value
    assert len(attempts) == failures + 1 and len(set(attempts)) == 1
    assert delays == DELAYS[:failures]
    assert sum(delays) <= 0.31
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("winerror", [5, 32, None])
def test_atomic_json_retry_exhaustion_keeps_original_error_and_old_target(
    tmp_path, replacement_boundary, winerror
):
    local_os, delays = replacement_boundary
    path = tmp_path / "pending-installation.json"
    path.write_bytes(b"old journal")
    attempts, errors = [], []

    def fail(source, target):
        attempts.append(source)
        errors.append(filesystem_error(winerror))
        assert target.read_bytes() == b"old journal"
        raise errors[-1]

    local_os.replace = fail
    with pytest.raises(OSError) as caught:
        install._atomic_json(path, {"phase": "compensating"})

    assert caught.value is errors[0]
    assert len(attempts) == 6 and len(set(attempts)) == 1
    assert delays == DELAYS
    assert path.read_bytes() == b"old journal"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("platform,winerror,code", [
    ("nt", 2, errno.ENOENT),
    ("nt", 19, errno.EACCES),  # A known different WinError must not use errno fallback.
    ("nt", None, errno.EIO),
    ("nt", None, errno.EPERM),
    ("posix", None, errno.EACCES),
    ("posix", 5, errno.EACCES),
    ("posix", 32, errno.EACCES),
])
def test_atomic_json_other_errors_are_not_retried(
    tmp_path, replacement_boundary, platform, winerror, code
):
    local_os, delays = replacement_boundary
    local_os.name = platform
    path = tmp_path / "pending-installation.json"
    path.write_bytes(b"old journal")
    error = filesystem_error(winerror, code)
    attempts = []

    def fail(source, target):
        attempts.append(source)
        raise error

    local_os.replace = fail
    with pytest.raises(OSError) as caught:
        install._atomic_json(path, {"phase": "compensating"})

    assert caught.value is error
    assert len(attempts) == 1
    assert not delays
    assert path.read_bytes() == b"old journal"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("stage", ["serialization", "write", "fsync"])
def test_atomic_json_pre_replace_failures_are_not_retried(
    tmp_path, monkeypatch, replacement_boundary, stage
):
    local_os, delays = replacement_boundary
    path = tmp_path / "pending-installation.json"
    path.write_bytes(b"old journal")
    error = TypeError("fixture serialization failure") if stage == "serialization" else filesystem_error(5)
    failures, attempts = [], []

    def fail(*args, **kwargs):
        failures.append(stage)
        raise error

    if stage == "fsync":
        local_os.fsync = fail
    elif stage == "write":
        fdopen = local_os.fdopen

        @contextmanager
        def failing_stream(*args, **kwargs):
            with fdopen(*args, **kwargs) as stream:
                yield SimpleNamespace(write=fail, flush=stream.flush, fileno=stream.fileno)

        local_os.fdopen = failing_stream
    else:
        monkeypatch.setattr(install, "json", SimpleNamespace(dump=fail))
    local_os.replace = lambda *args: attempts.append(args)
    with pytest.raises(type(error)) as caught:
        install._atomic_json(path, {"phase": "compensating"})

    assert caught.value is error
    assert failures == [stage]
    assert not attempts and not delays
    assert path.read_bytes() == b"old journal"
    assert list(tmp_path.iterdir()) == [path]
