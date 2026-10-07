import hashlib
import json
import posixpath
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from uuid import UUID
from contextlib import redirect_stdout
from io import StringIO

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.remote.files import RemoteFileService
from hpc_mcp.ssh.protocol import CommandResult


RID = UUID('12345678-1234-5678-1234-567812345678')
REAL_RUN = subprocess.run


class FakeSftp:
    def __init__(self, local_root: Path, remote_root: str):
        self.local_root = local_root
        self.remote_root = remote_root
        self.uploads = 0

    def _local(self, path: str) -> Path:
        normalized = posixpath.normpath(path)
        if normalized == self.remote_root:
            return self.local_root
        prefix = f"{self.remote_root}/"
        if not normalized.startswith(prefix):
            raise AssertionError(f"fake SFTP path is outside remote root: {path}")
        relative = PurePosixPath(normalized.removeprefix(prefix))
        return self.local_root.joinpath(*relative.parts)

    async def lstat(self, path):
        st = self._local(path).lstat()
        return SimpleNamespace(permissions=st.st_mode, size=st.st_size)

    async def open(self, path, mode, **kwargs):
        if 'x' in mode:
            self.uploads += 1
        return open(self._local(path), mode)

    async def makedirs(self, path, exist_ok=False):
        self._local(path).mkdir(parents=True, exist_ok=exist_ok)

    async def chmod(self, path, mode):
        self._local(path).chmod(mode)

    async def posix_rename(self, source, target):
        self._local(source).replace(self._local(target))

    async def remove(self, path):
        self._local(path).unlink()


class FakeRemote:
    def __init__(self, root: Path):
        self.root = root
        self.remote_root = '/srv/projects/project with spaces'
        self.transport = FakeSftp(root, self.remote_root)
        self.commands = []
        self.events = []
        self.disconnect = None
        self.sbatch_calls = 0
        self.failure = False

    async def realpath(self, path):
        return posixpath.normpath(path)

    def localize_args(self, args: list[str]) -> list[str]:
        localized = list(args)
        helper = localized.index('-I') + 1
        localized[helper] = str(self.transport._local(localized[helper]))
        project_root = localized.index('--project-root') + 1
        localized[project_root] = str(self.transport._local(localized[project_root]))
        return localized

    @asynccontextmanager
    async def sftp(self):
        self.events.append('sftp')
        yield self.transport

    async def run(self, command):
        from hpc_mcp.slurm.submit_once import main
        self.commands.append(command)
        self.events.append('run')
        mode, self.disconnect = self.disconnect, None
        if mode == 'before':
            raise HpcMcpError('SSH_TRANSPORT_FAILED', 'private diagnostic')
        output = StringIO()
        with redirect_stdout(output):
            arguments = self.localize_args(list(command.args))
            status = main(arguments[arguments.index('--project-root'):])
        if mode == 'after':
            raise HpcMcpError('SSH_TRANSPORT_FAILED', 'private diagnostic')
        return CommandResult(exit_status=status, stdout=output.getvalue(), stderr='')


@pytest.fixture
def service(tmp_path, monkeypatch):
    from hpc_mcp.slurm.submit import SlurmSubmitService
    root = tmp_path / 'project with spaces'
    (root / 'slurm').mkdir(parents=True)
    (root / 'slurm/run.slurm').write_text('#!/bin/bash\n#SBATCH --time=10\necho test\n')
    remote = FakeRemote(root)
    registry = {'project-1': SimpleNamespace(remote_root=remote.remote_root)}
    def fake_sbatch(args, **kwargs):
        remote.sbatch_calls += 1
        return subprocess.CompletedProcess(args, 1 if remote.failure else 0, '81234\n', '')
    monkeypatch.setattr(subprocess, 'run', fake_sbatch)
    return SlurmSubmitService(registry, remote, RemoteFileService(registry, remote)), remote


async def test_repeated_request_id_returns_same_job_without_second_sbatch(service):
    submit_service, remote = service
    first = await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    second = await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert first == second
    assert first.job_id == '81234'
    assert remote.sbatch_calls == 1
    assert remote.transport.uploads == 1
    command = remote.commands[0]
    assert command.program == 'python3'
    assert command.args == ('-I', f'{remote.remote_root}/.hpc/bin/submit_once.py', '--project-root', remote.remote_root, '--script', 'slurm/run.slurm', '--request-id', str(RID))
    assert command.cwd == remote.remote_root
    assert not command.environment


@pytest.mark.parametrize('when,run_count', [('before', 2), ('after', 1)])
async def test_disconnect_recovers_receipt_before_retry(service, when, run_count):
    submit_service, remote = service
    remote.disconnect = when
    receipt = await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert receipt.job_id == '81234'
    assert remote.sbatch_calls == 1
    assert len(remote.commands) == run_count
    if when == 'before':
        positions = [i for i, event in enumerate(remote.events) if event == 'run']
        assert 'sftp' in remote.events[positions[0] + 1:positions[1]]


async def test_changed_helper_is_redeployed(service):
    submit_service, remote = service
    await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    path = remote.root / '.hpc/bin/submit_once.py'
    path.write_text('outdated')
    await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert remote.transport.uploads == 2
    from hpc_mcp.slurm import submit_once
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(Path(submit_once.__file__).read_bytes()).digest()


async def test_unrecoverable_disconnect_is_unknown_without_raw_diagnostics(service, monkeypatch):
    submit_service, remote = service
    async def disconnected(command):
        remote.events.append('run')
        raise HpcMcpError('SSH_TIMEOUT', 'secret /private/path')
    monkeypatch.setattr(remote, 'run', disconnected)
    with pytest.raises(HpcMcpError) as error:
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert error.value.code == 'SUBMISSION_UNKNOWN'
    assert 'secret' not in str(error.value)
    assert remote.events.count('run') == 2


async def test_confirmed_nonzero_is_failure_not_unknown(service):
    submit_service, remote = service
    remote.failure = True
    with pytest.raises(HpcMcpError) as error:
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert error.value.code == 'SUBMISSION_FAILED'
    assert len(remote.commands) == 1


@pytest.mark.parametrize('script', ['../outside', '/tmp/outside', 'slurm/x\n.slurm'])
async def test_invalid_path_never_invokes_helper(service, script):
    submit_service, remote = service
    with pytest.raises((ValueError, HpcMcpError)):
        await submit_service.submit('project-1', script, RID)
    assert not remote.commands


async def test_preview_rejects_invalid_script_before_submission(service):
    submit_service, remote = service
    (remote.root / 'slurm/run.slurm').write_text('not a batch script')
    with pytest.raises(ValueError):
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert not remote.commands


async def test_metadata_symlink_rejected(service, tmp_path):
    submit_service, remote = service
    outside = tmp_path / 'outside'
    outside.mkdir()
    (remote.root / '.hpc').symlink_to(outside)
    with pytest.raises(HpcMcpError):
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert not remote.commands
    assert not list(outside.iterdir())


async def test_oversized_stale_helper_is_replaced(service):
    submit_service, remote = service
    directory = remote.root / '.hpc/bin'
    directory.mkdir(parents=True)
    (directory / 'submit_once.py').write_bytes(b'x' * 100000)
    receipt = await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert receipt.job_id == '81234'
    assert remote.transport.uploads == 1


async def test_recovery_read_failure_forbids_retry(service, monkeypatch):
    submit_service, remote = service
    original = remote.transport.lstat
    async def failed_recovery(path):
        if remote.commands and path.endswith(f'{RID}.json'):
            raise PermissionError('secret path')
        return await original(path)
    monkeypatch.setattr(remote.transport, 'lstat', failed_recovery)
    remote.disconnect = 'before'
    with pytest.raises(HpcMcpError) as error:
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert error.value.code == 'SUBMISSION_UNKNOWN'
    assert len(remote.commands) == 1
    assert remote.sbatch_calls == 0


async def test_corrupt_recovered_receipt_forbids_retry(service, monkeypatch):
    submit_service, remote = service
    async def disconnected(command):
        remote.commands.append(command)
        (remote.root / f'.hpc/submissions/{RID}.json').write_text('{bad')
        raise HpcMcpError('SSH_TRANSPORT_FAILED', 'secret')
    monkeypatch.setattr(remote, 'run', disconnected)
    with pytest.raises(HpcMcpError) as error:
        await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    assert error.value.code == 'SUBMISSION_UNKNOWN'
    assert len(remote.commands) == 1


async def test_hyphen_prefixed_script_has_unambiguous_argument(service):
    submit_service, remote = service
    (remote.root / '-run.slurm').write_text('#!/bin/bash\ntrue\n')
    receipt = await submit_service.submit('project-1', '-run.slurm', RID)
    assert receipt.script == '-run.slurm'
    args = remote.commands[0].args
    assert args[args.index('--script') + 1] == './-run.slurm'


async def test_deployed_helper_cannot_import_neighboring_shadow_module(service):
    submit_service, remote = service
    await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    marker = remote.root / 'shadow-executed'
    (remote.root / '.hpc/bin/argparse.py').write_text(
        f'from pathlib import Path\nPath({str(marker)!r}).touch()\nraise RuntimeError("shadow")\n'
    )
    args = list(remote.commands[0].args)
    # Invalid script ensures this real helper process can never dispatch sbatch.
    args[args.index('--script') + 1] = '../outside.slurm'
    result = REAL_RUN(
        [sys.executable, *remote.localize_args(args)],
        cwd=remote.root,
        capture_output=True,
        text=True,
    )
    assert not marker.exists()
    assert result.returncode == 3
    assert json.loads(result.stdout) == {'error': 'SUBMISSION_INVALID'}


async def test_successful_dispatch_then_corrupt_receipt_requires_reconciliation(service):
    submit_service, remote = service
    await submit_service.submit('project-1', 'slurm/run.slurm', RID)
    (remote.root / f'.hpc/submissions/{RID}.json').write_text('{corrupt')
    for _ in range(2):
        with pytest.raises(HpcMcpError) as error:
            await submit_service.submit('project-1', 'slurm/run.slurm', RID)
        assert error.value.code == 'SUBMISSION_UNKNOWN'
        assert 'Reconcile' in error.value.details['recovery']
    assert remote.sbatch_calls == 1
    assert (remote.root / f'.hpc/submissions/{RID}.pending').exists()
