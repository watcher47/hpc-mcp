import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.policy import CommandPolicy, RemoteCommand
from hpc_mcp.ssh.protocol import CommandResult


def probe_source():
    from hpc_mcp.slurm import resource_probe
    return Path(resource_probe.__file__).read_bytes()


def test_stdin_policy_is_exact_packaged_probe_only():
    command = RemoteCommand('python3', ['-I', '-', 'inventory'], '/project', standard_input=probe_source())
    assert CommandPolicy().validate(command).standard_input == probe_source()
    assert 'standard_input' not in command.model_dump() and 'standard_input' not in repr(command)
    for changes in ({'origin':'user'}, {'standard_input':b'print(1)'}, {'args':('-I','-','guard')}, {'program':'pwd'}, {'environment':{'X':'Y'}}):
        with pytest.raises(HpcMcpError): CommandPolicy().validate(command.model_copy(update=changes))


async def test_gpu_inventory_and_preview_never_write_remote(tmp_path):
    from hpc_mcp.slurm.gpu import GpuSelection, GpuService
    from hpc_mcp.container import PreviewService
    commands = []
    class Ssh:
        async def realpath(self, path): return path
        @asynccontextmanager
        async def sftp(self):
            yield SimpleNamespace(lstat=lambda path: SimpleNamespace(permissions=0o40755))
        async def run(self, command):
            CommandPolicy().validate(command)
            commands.append(command)
            return CommandResult(exit_status=76, stdout=json.dumps({'status':'unknown', 'admission':{'allowed':False}, 'selection':None, 'reasons':['unmapped']}), stderr='')
    class Files:
        async def read(self, *args, **kwargs):
            return SimpleNamespace(size=26, content='#!/bin/bash\necho business\n', next_offset=None)
    gpu = GpuService({'p':SimpleNamespace(remote_root='/project')}, Ssh())
    assert (await gpu.inventory('p'))['status'] == 'unknown'
    preview = await PreviewService(Files(), gpu).preview('p', 'job.sh', gpu_selection=GpuSelection(mode='auto', count=1))
    assert not preview.gpu['submittable'] and preview.gpu['guard_required']
    assert commands[0].args == ('-I', '-', 'inventory')
    assert commands[1].args[:3] == ('-I', '-', 'preflight')
    assert all(command.standard_input == probe_source() for command in commands)


@pytest.mark.parametrize('exit_status,payload,truncated', [(0, '{}', False), (0, '[]', False), (0, '{bad', False), (0, '{"status":"busy"}', False), (76, '{"status":"unknown"}', True)])
async def test_gpu_inventory_fails_closed_on_bad_protocol(exit_status, payload, truncated):
    from hpc_mcp.slurm.gpu import GpuService
    class Ssh:
        async def realpath(self, path): return path
        @asynccontextmanager
        async def sftp(self): yield SimpleNamespace(lstat=lambda path: SimpleNamespace(permissions=0o40755))
        async def run(self, command): return CommandResult(exit_status=exit_status, stdout=payload, stderr='', truncated=truncated)
    with pytest.raises(HpcMcpError): await GpuService({'p':SimpleNamespace(remote_root='/project')}, Ssh()).inventory('p')
