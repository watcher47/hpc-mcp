import errno
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import UUID

import pytest


RID = UUID('12345678-1234-5678-1234-567812345678')


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / 'project'
    (root / 'slurm').mkdir(parents=True)
    (root / 'slurm/run.slurm').write_text('#!/bin/bash\necho test\n')
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        time.sleep(0.01)
        return subprocess.CompletedProcess(args, 0, '81234\n', '')

    monkeypatch.setattr(subprocess, 'run', fake_run)
    return root, calls


def test_duplicate_and_concurrent_requests_submit_once(project):
    from hpc_mcp.slurm.submit_once import submit_once
    root, calls = project
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(lambda _: submit_once(root, 'slurm/run.slurm', RID), range(4)))
    assert all(receipt == receipts[0] for receipt in receipts)
    assert receipts[0]['job_id'] == '81234'
    assert set(receipts[0]) == {'request_id', 'job_id', 'script', 'submitted_at'}
    assert len(calls) == 1
    assert calls[0] == (['sbatch', '--parsable', str(root / 'slurm/run.slurm')],
                        {'cwd': str(root), 'capture_output': True, 'text': True})
    assert json.loads((root / f'.hpc/submissions/{RID}.json').read_text()) == receipts[0]


@pytest.mark.parametrize('script', ['../outside.slurm', '/tmp/outside.slurm', 'slurm/bad\n.slurm'])
def test_unsafe_script_rejected_before_sbatch(project, script):
    from hpc_mcp.slurm.submit_once import submit_once
    root, calls = project
    with pytest.raises(ValueError):
        submit_once(root, script, RID)
    assert not calls


@pytest.mark.parametrize('link', ['script', '.hpc', 'submissions', 'lock', 'receipt'])
def test_symlink_escape_rejected(project, tmp_path, link):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once
    root, calls = project
    outside = tmp_path / 'outside'
    outside.mkdir()
    script = 'slurm/run.slurm'
    if link == 'script':
        (outside / 'run.slurm').write_text('#!/bin/bash\ntrue\n')
        (root / 'escape.slurm').symlink_to(outside / 'run.slurm')
        script = 'escape.slurm'
    elif link == '.hpc':
        (root / '.hpc').symlink_to(outside)
    else:
        (root / '.hpc').mkdir()
        if link == 'submissions':
            (root / '.hpc/submissions').symlink_to(outside)
        else:
            (root / '.hpc/submissions').mkdir()
            target = outside / 'target'
            target.write_text('{}')
            suffix = 'lock' if link == 'lock' else 'json'
            (root / f'.hpc/submissions/{RID}.{suffix}').symlink_to(target)
    with pytest.raises(ValueError if link == 'script' else SubmissionUnknown):
        submit_once(root, script, RID)
    assert not calls


def test_windows_junction_metadata_escape_rejected(project, tmp_path):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once

    root, calls = project
    outside = tmp_path / 'outside'
    outside.mkdir()
    junction = root / '.hpc'

    if os.name == 'nt':
        subprocess.check_call(
            ['cmd.exe', '/d', '/c', 'mklink', '/J', str(junction), str(outside)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            with pytest.raises(SubmissionUnknown):
                submit_once(root, 'slurm/run.slurm', RID)
        finally:
            os.rmdir(junction)
    else:
        # The Windows-specific detector is exercised portably when a real
        # junction cannot exist on the current host.
        from hpc_mcp.slurm import submit_once as helper

        junction.mkdir()
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            helper, '_is_reparse_point', lambda path: path == junction, raising=False
        )
        try:
            with pytest.raises(SubmissionUnknown):
                submit_once(root, 'slurm/run.slurm', RID)
        finally:
            monkeypatch.undo()

    assert not calls
    assert not (outside / 'submissions').exists()


def test_windows_lock_initialization_is_serialized_across_independent_fds(
    tmp_path, monkeypatch
):
    from hpc_mcp.slurm import submit_once as helper

    lock_path = tmp_path / 'request.lock'
    first_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT)
    second_fd = os.open(lock_path, os.O_RDWR)
    gate = threading.Lock()
    both_saw_empty = threading.Barrier(2)
    first_lock_attempt = threading.Event()
    first_inside = threading.Event()
    release_first = threading.Event()
    owner = {'fd': None}
    initial_stats = set()
    real_fstat = os.fstat
    real_write = os.write

    fake_msvcrt = SimpleNamespace(LK_NBLCK=1, LK_UNLCK=2)

    def locking(fd, mode, count):
        assert count == 1
        if mode == fake_msvcrt.LK_UNLCK:
            with gate:
                assert owner['fd'] == fd
                owner['fd'] = None
            return
        assert mode == fake_msvcrt.LK_NBLCK
        if fd == first_fd:
            first_lock_attempt.set()
        elif not first_lock_attempt.wait(timeout=2):
            raise AssertionError('first descriptor never attempted the lock')
        with gate:
            if owner['fd'] is None:
                owner['fd'] = fd
                return
        raise OSError(errno.EACCES, 'byte is locked by another descriptor')

    fake_msvcrt.locking = locking

    def synchronized_fstat(fd):
        attrs = real_fstat(fd)
        with gate:
            before_lock = owner['fd'] is None
            synchronize = (
                fd in {first_fd, second_fd}
                and fd not in initial_stats
                and before_lock
            )
            if synchronize:
                initial_stats.add(fd)
        if synchronize:
            both_saw_empty.wait(timeout=2)
        return attrs

    def mandatory_locking_write(fd, data):
        if fd == second_fd and not first_inside.wait(timeout=2):
            raise AssertionError('first descriptor never entered its critical section')
        with gate:
            locked_by_other = owner['fd'] not in {None, fd}
        if locked_by_other:
            raise OSError(errno.EACCES, 'write overlaps another descriptor lock')
        return real_write(fd, data)

    monkeypatch.setattr(helper.os, 'name', 'nt')
    monkeypatch.setattr(helper.os, 'fstat', synchronized_fstat)
    monkeypatch.setattr(helper.os, 'write', mandatory_locking_write)
    monkeypatch.setitem(sys.modules, 'msvcrt', fake_msvcrt)

    def first_caller():
        with helper._exclusive_lock(first_fd):
            first_inside.set()
            assert release_first.wait(timeout=2)

    def second_caller():
        with helper._exclusive_lock(second_fd):
            return None

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(first_caller)
            second = pool.submit(second_caller)
            assert first_inside.wait(timeout=2)
            release_first.set()
            first.result(timeout=2)
            second.result(timeout=2)
    finally:
        release_first.set()
        os.close(second_fd)
        os.close(first_fd)


@pytest.mark.parametrize('output', ['81234;cluster\n', '81234\n9999\n', '81234_1x', '', ' 81234\n'])
def test_invalid_job_id_is_unknown_and_never_retried(project, monkeypatch, output):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once
    root, calls = project
    def invalid(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, output, '')
    monkeypatch.setattr(subprocess, 'run', invalid)
    for _ in range(2):
        with pytest.raises(SubmissionUnknown):
            submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 1
    assert not (root / f'.hpc/submissions/{RID}.json').exists()


def test_confirmed_failure_can_be_retried(project, monkeypatch):
    from hpc_mcp.slurm.submit_once import SubmissionFailed, submit_once
    root, calls = project
    def failed(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, '', 'private diagnostic')
    monkeypatch.setattr(subprocess, 'run', failed)
    for _ in range(2):
        with pytest.raises(SubmissionFailed, match='Slurm submission failed'):
            submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 2


def test_receipt_write_failure_preserves_unknown_state(project, monkeypatch):
    from hpc_mcp.slurm import submit_once as helper
    root, calls = project
    monkeypatch.setattr(helper.os, 'replace', lambda *a, **k: (_ for _ in ()).throw(OSError('disk')))
    for _ in range(2):
        with pytest.raises(helper.SubmissionUnknown):
            helper.submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 1


def test_request_id_cannot_be_reused_for_other_script(project):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once
    root, calls = project
    submit_once(root, 'slurm/run.slurm', RID)
    (root / 'other.slurm').write_text('#!/bin/bash\ntrue\n')
    with pytest.raises(SubmissionUnknown):
        submit_once(root, 'other.slurm', RID)
    assert len(calls) == 1


def test_invalid_request_id_rejected(project):
    from hpc_mcp.slurm.submit_once import submit_once
    root, calls = project
    with pytest.raises(ValueError):
        submit_once(root, 'slurm/run.slurm', '../escape')
    assert not calls


def test_array_job_id_accepted(project, monkeypatch):
    from hpc_mcp.slurm.submit_once import submit_once
    root, _ = project
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a, 0, '81234_2\n', ''))
    assert submit_once(root, 'slurm/run.slurm', RID)['job_id'] == '81234_2'


def test_cli_outputs_exact_receipt_without_diagnostics(project, capsys):
    from hpc_mcp.slurm.submit_once import main
    root, calls = project
    assert main(['--project-root', str(root), '--script', 'slurm/run.slurm', '--request-id', str(RID)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)['job_id'] == '81234'
    assert captured.err == ''
    assert len(calls) == 1


@pytest.mark.parametrize('changes', [
    {'job_id': '81234\n'}, {'request_id': str(UUID(int=1))},
    {'script': 'other.slurm'}, {'submitted_at': 'invalid'}, {'job_id': 81234},
    {'extra': 'field'},
])
def test_corrupt_receipt_never_submits_again(project, changes):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once
    root, calls = project
    receipt = submit_once(root, 'slurm/run.slurm', RID)
    receipt.update(changes)
    (root / f'.hpc/submissions/{RID}.json').write_text(json.dumps(receipt))
    with pytest.raises(SubmissionUnknown):
        submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 1


def test_killed_sbatch_is_unknown(project, monkeypatch):
    from hpc_mcp.slurm.submit_once import SubmissionUnknown, submit_once
    root, calls = project
    def killed(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, -9, '', '')
    monkeypatch.setattr(subprocess, 'run', killed)
    for _ in range(2):
        with pytest.raises(SubmissionUnknown):
            submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 1


def test_oserror_after_sbatch_acceptance_is_unknown_and_never_retried(project, monkeypatch):
    from hpc_mcp.slurm import submit_once as helper
    root, calls = project

    def accepted_then_disconnected(args, **kwargs):
        calls.append(args)
        raise OSError('connection reset after sbatch accepted the job')

    monkeypatch.setattr(subprocess, 'run', accepted_then_disconnected)
    for _ in range(2):
        with pytest.raises(helper.SubmissionUnknown):
            helper.submit_once(root, 'slurm/run.slurm', RID)
    assert len(calls) == 1
    assert (root / f'.hpc/submissions/{RID}.pending').exists()


def test_metadata_directory_entries_are_durable_before_sbatch(project, monkeypatch):
    from hpc_mcp.slurm import submit_once as helper
    root, _ = project
    synchronized = set()
    original = helper.os.fsync
    def fsync(fd):
        synchronized.add(helper.os.fstat(fd).st_ino)
        original(fd)
    def submitted(args, **kwargs):
        if os.name == 'nt':
            # Windows cannot open/fsync directories. The fallback still makes
            # the pending marker durable before invoking sbatch.
            pending = root / f'.hpc/submissions/{RID}.pending'
            assert pending.stat().st_ino in synchronized
        else:
            assert root.stat().st_ino in synchronized
            assert (root / '.hpc').stat().st_ino in synchronized
            assert (root / '.hpc/submissions').stat().st_ino in synchronized
        return subprocess.CompletedProcess(args, 0, '81234\n', '')
    monkeypatch.setattr(helper.os, 'fsync', fsync)
    monkeypatch.setattr(subprocess, 'run', submitted)
    helper.submit_once(root, 'slurm/run.slurm', RID)


@pytest.mark.parametrize('state', ['json', 'oversize', 'encoding', 'unreadable', 'pending'])
def test_cli_persisted_state_failure_is_unknown(project, monkeypatch, capsys, state):
    from hpc_mcp.slurm import submit_once as helper
    root, calls = project
    helper.submit_once(root, 'slurm/run.slurm', RID)
    receipt = root / f'.hpc/submissions/{RID}.json'
    if state == 'pending':
        receipt.unlink()
        (root / f'.hpc/submissions/{RID}.pending').write_bytes(b'\xff')
    elif state == 'unreadable':
        original = helper._open_regular
        def denied(name, flags, directory):
            if name.endswith('.json'):
                raise PermissionError('private diagnostic')
            return original(name, flags, directory)
        monkeypatch.setattr(helper, '_open_regular', denied)
    else:
        receipt.write_bytes({'json': b'{bad', 'oversize': b'x' * (helper.RECEIPT_LIMIT + 1), 'encoding': b'\xff'}[state])
    for _ in range(2):
        assert helper.main(['--project-root', str(root), '--script', 'slurm/run.slurm', '--request-id', str(RID)]) == 2
        captured = capsys.readouterr()
        assert json.loads(captured.out) == {'error': 'SUBMISSION_UNKNOWN'}
        assert captured.err == ''
    assert len(calls) == 1
    assert (root / f'.hpc/submissions/{RID}.pending').exists()


@pytest.mark.parametrize('stage', ['decode', 'receipt'])
def test_post_dispatch_processing_failure_is_unknown(project, monkeypatch, capsys, stage):
    from hpc_mcp.slurm import submit_once as helper
    root, calls = project
    if stage == 'decode':
        def accepted_but_undecodable(args, **kwargs):
            calls.append(args)
            raise UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'private diagnostic')
        monkeypatch.setattr(subprocess, 'run', accepted_but_undecodable)
    else:
        def bad_receipt(*args, **kwargs):
            raise ValueError('private diagnostic')
        monkeypatch.setattr(helper, '_atomic_receipt', bad_receipt)
    for _ in range(2):
        assert helper.main(['--project-root', str(root), '--script', 'slurm/run.slurm', '--request-id', str(RID)]) == 2
        assert json.loads(capsys.readouterr().out) == {'error': 'SUBMISSION_UNKNOWN'}
    assert len(calls) == 1
    assert (root / f'.hpc/submissions/{RID}.pending').exists()


def test_cli_confirmed_invalid_request_never_dispatches(project, capsys):
    from hpc_mcp.slurm.submit_once import main
    root, calls = project
    assert main(['--project-root', str(root), '--script', '../outside.slurm', '--request-id', str(RID)]) == 3
    assert json.loads(capsys.readouterr().out) == {'error': 'SUBMISSION_INVALID'}
    assert not calls
