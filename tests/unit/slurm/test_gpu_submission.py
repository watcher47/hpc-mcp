import json
import sys
import hashlib
import subprocess
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

import pytest

from hpc_mcp.slurm import resource_probe, submit_once as helper
from hpc_mcp.slurm.gpu import GpuSelection
from hpc_mcp.slurm.gpu_submission import wrapper

RID = UUID('12345678-1234-5678-1234-567812345678')
SELECTOR = {'mode': 'auto', 'count': 2}
PLAN = {'node': 'node1', 'gres': 'gpu:card_a:1,gpu:card_b:1', 'gpus': [
    {'index': 4, 'uuid': 'GPU-a', 'minor': 2, 'gres_type': 'card_a'},
    {'index': 0, 'uuid': 'GPU-b', 'minor': 7, 'gres_type': 'card_b'}]}


@pytest.fixture
def gpu_project(tmp_path, monkeypatch):
    root = tmp_path / 'project with spaces'
    root.mkdir()
    (root / 'job.sh').write_text('#!/bin/bash\n#SBATCH --output=logs/%j.out\necho business\n')
    calls = []
    probes = []
    def evaluate(snapshot, selection):
        probes.append(selection)
        return {'status': 'ready', 'admission': {'allowed': True}, 'selection': PLAN, 'reasons': []}
    monkeypatch.setattr(resource_probe, 'collect_snapshot', lambda: {'sampled_at': 'now'})
    monkeypatch.setattr(resource_probe, 'evaluate_snapshot', evaluate)
    probe_source = Path(resource_probe.__file__).read_bytes()
    bin_dir = root / '.hpc/bin'
    bin_dir.mkdir(parents=True)
    probe_path = bin_dir / ('resource_probe.'+hashlib.sha256(probe_source).hexdigest()+'.py')
    probe_path.write_bytes(probe_source)
    monkeypatch.setattr(resource_probe, '__file__', str(probe_path))
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, '81234\n', '')
    monkeypatch.setattr(subprocess, 'run', run)
    return root, calls, probes


def submit(root, selector=SELECTOR):
    return helper.submit_once(root, 'job.sh', RID, gpu_selection=selector, gpu_probe=resource_probe)


@pytest.mark.parametrize('gpu_first', [False, True], ids=['cpu-to-gpu', 'gpu-to-cpu'])
def test_accepted_receipt_rejects_submission_mode_change(gpu_project, gpu_first):
    root, calls, probes = gpu_project
    if gpu_first:
        submit(root)
    else:
        helper.submit_once(root, 'job.sh', RID)
    expected_probes = 1 if gpu_first else 0
    with pytest.raises(helper.SubmissionConflict):
        if gpu_first:
            helper.submit_once(root, 'job.sh', RID)
        else:
            submit(root)
    assert len(calls) == 1
    assert len(probes) == expected_probes


def test_uncertain_gpu_submission_does_not_probe_or_dispatch_again(gpu_project, monkeypatch):
    root, calls, probes = gpu_project
    def uncertain_dispatch(argv, **kwargs):
        calls.append((argv, kwargs))
        raise OSError('dispatch outcome unavailable')
    monkeypatch.setattr(subprocess, 'run', uncertain_dispatch)
    with pytest.raises(helper.SubmissionUnknown):
        submit(root)
    assert (root / '.hpc/submissions' / f'{RID}.pending').exists()
    with pytest.raises(helper.SubmissionUnknown):
        submit(root)
    assert len(calls) == len(probes) == 1


def test_verified_gpu_rules_load_in_isolated_python(tmp_path):
    from hpc_mcp.slurm import gpu_submission
    root = tmp_path / 'isolated project'
    bin_dir = root / '.hpc/bin'
    bin_dir.mkdir(parents=True)
    source = Path(gpu_submission.__file__).read_bytes()
    digest = hashlib.sha256(source).hexdigest()
    rules_path = bin_dir / f'gpu_submission.{digest}.py'
    rules_path.write_bytes(source)
    child_code = '''
import importlib.abc, importlib.util, json, sys
from pathlib import Path
class RejectInstalledPackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "hpc_mcp" or fullname.startswith("hpc_mcp."):
            raise ModuleNotFoundError("hpc_mcp is deliberately unavailable")
sys.meta_path.insert(0, RejectInstalledPackage())
spec = importlib.util.spec_from_file_location("isolated_submit_once", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
rules = module._load_verified(Path(sys.argv[2]), sys.argv[3], sys.argv[4])
print(json.dumps(rules.canonical_selector({"mode":"exact", "indices":[4,0]})))
'''
    result = subprocess.run([sys.executable, '-I', '-c', child_code,
        str(Path(helper.__file__).resolve()), str(root), str(rules_path), digest],
        cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'mode':'exact', 'indices':[0,4]}


def test_gpu_submits_exact_argv_and_freezes_guard_before_business(gpu_project):
    root, calls, probes = gpu_project
    original = (root / 'job.sh').read_bytes()
    receipt = submit(root)
    wrapper = root / receipt['submission_script']
    text = wrapper.read_text()
    assert calls[0][0] == ['sbatch', '--parsable', '--nodelist=node1', '--nodes=1', '--gres='+PLAN['gres'], str(wrapper)]
    assert calls[0][1]['cwd'] == str(root)
    assert text.index(' guard ') < text.index('echo business')
    assert '#SBATCH --output=logs/%j.out' in text
    assert 'CUDA_DEVICE_ORDER=PCI_BUS_ID' in text
    assert 'CUDA_VISIBLE_DEVICES' not in text
    assert '$0' not in text and 'BASH_SOURCE' not in text
    assert str(RID) in receipt['guard_report'] and '${SLURM_JOB_ID}' in receipt['guard_report']
    assert receipt['gpu_plan'] == PLAN and receipt['version'] == 2
    assert (root / 'job.sh').read_bytes() == original
    assert json.loads((root / receipt['expected_plan']).read_text()) == PLAN
    assert len(probes) == 1


def test_concurrent_gpu_replay_uses_original_plan_without_new_probe(gpu_project):
    root, calls, probes = gpu_project
    with ThreadPoolExecutor(max_workers=3) as pool:
        receipts = list(pool.map(lambda _: submit(root), range(3)))
    assert receipts[0] == receipts[1] == receipts[2]
    assert len(calls) == len(probes) == 1


@pytest.mark.parametrize('change', ['selector', 'script'])
def test_gpu_replay_conflict_before_probe_or_sbatch(gpu_project, change):
    root, calls, probes = gpu_project
    submit(root)
    if change == 'script':
        (root / 'job.sh').write_text('#!/bin/bash\necho changed\n')
    with pytest.raises(helper.SubmissionConflict):
        submit(root, {'mode': 'auto', 'count': 1} if change == 'selector' else SELECTOR)
    assert len(calls) == len(probes) == 1


@pytest.mark.parametrize('status', ['busy', 'unknown'])
def test_rejected_probe_leaves_no_pending_or_sbatch(gpu_project, monkeypatch, status):
    root, calls, probes = gpu_project
    monkeypatch.setattr(resource_probe, 'evaluate_snapshot', lambda *a: {'status':status, 'admission':{'allowed':False}, 'selection':None})
    with pytest.raises(helper.GpuRejected):
        submit(root)
    assert not calls and not (root / f'.hpc/submissions/{RID}.pending').exists()


@pytest.mark.parametrize('body', [
    '#!/bin/sh\necho bad\n', '#!/bin/bash\n#SBATCH --nodes=2\necho bad\n',
    '#!/bin/bash\n#SBATCH --gpus=2\necho bad\n', '#!/bin/bash\n#SBATCH --gres=gpu:2\necho bad\n',
    '#!/bin/bash\n#SBATCH --array=1-2\necho bad\n', '#!/bin/bash\n#SBATCH -D /tmp\necho bad\n',
    '#!/bin/bash\necho bad\n#SBATCH --gres=gpu:card_a:1\n',
    '#!/bin/bash\n#SBATCH --gres=license:foo:1\necho bad\n',
    '#!/bin/bash\n#SBATCH --nodelist=other\necho bad\n',
    '#!/bin/bash\n#SBATCH --nodes=1 --array=1-2\necho bad\n',
])
def test_unsupported_script_rejected_before_dispatch(gpu_project, body):
    root, calls, probes = gpu_project
    (root / 'job.sh').write_text(body)
    with pytest.raises(helper.GpuRejected):
        submit(root)
    assert not calls and not (root / f'.hpc/submissions/{RID}.pending').exists()


def test_equal_gres_is_allowed_and_sbatch_environment_removed(gpu_project, monkeypatch):
    root, calls, probes = gpu_project
    (root / 'job.sh').write_text('#!/bin/bash\n#SBATCH --gres='+PLAN['gres']+'\necho business\n')
    monkeypatch.setenv('SBATCH_GRES', 'gpu:other:1')
    submit(root)
    assert not any(k.startswith('SBATCH_') for k in calls[0][1]['env'])


def test_confirmed_failure_retry_reuses_immutable_snapshots(gpu_project, monkeypatch):
    root, calls, probes = gpu_project
    original = subprocess.run
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a[0], 1, '', ''))
    with pytest.raises(helper.SubmissionFailed): submit(root)
    frozen = (root / f'.hpc/submissions/{RID}.sh').read_bytes()
    monkeypatch.setattr(subprocess, 'run', original)
    receipt = submit(root)
    assert (root / receipt['submission_script']).read_bytes() == frozen
    assert len(probes) == 1


def test_gpu_receipt_rejects_path_escape(gpu_project):
    root, calls, probes = gpu_project
    receipt = submit(root)
    receipt['submission_script'] = '../escape.sh'
    with pytest.raises(ValueError): helper.validate_receipt(receipt, str(RID), 'job.sh')


@pytest.mark.parametrize('guard_status', [0, 75, 76])
def test_spooled_bash_script_really_stops_before_business(tmp_path, guard_status):
    bash = ('C:/Program Files/Git/bin/bash.exe' if os.name == 'nt'
            else shutil.which('bash'))
    if bash is None or not Path(bash).is_file():
        pytest.skip('Git Bash on Windows or Bash on POSIX is required for this execution check')
    import sys
    root = tmp_path / 'project with spaces'
    root.mkdir()
    probe = root / 'probe.py'
    probe.write_text(f'import sys\nsys.exit({guard_status})\n', encoding='utf-8')
    spool = tmp_path / 'slurm spool'
    spool.mkdir()
    script = spool / 'slurm_script'
    source = '#!/bin/bash\nprintf "%s" "$CUDA_VISIBLE_DEVICES" > business.txt\n'
    script.write_text(wrapper(source, root, probe,
        hashlib.sha256(probe.read_bytes()).hexdigest(), root / 'expected.json',
        root / 'guard-${SLURM_JOB_ID}.json'), encoding='utf-8', newline='\n')
    python = sys.executable.replace('\\', '/')
    if os.name == 'nt':
        python = '/' + python[0].lower() + python[2:]
    # Only the guard process is stubbed; execute the actual generated Bash.
    import shlex
    launcher = 'python3() { ' + shlex.quote(python) + ' "$@"; }; export -f python3; bash "$1"'
    result = subprocess.run([bash, '-c', launcher, 'launcher', str(script)], cwd=root,
        env=dict(os.environ, SLURM_JOB_ID='81234', CUDA_VISIBLE_DEVICES='slurm-mapped'),
        capture_output=True, text=True, timeout=20)
    assert result.returncode == guard_status, result.stderr
    business = root / 'business.txt'
    assert business.exists() == (guard_status == 0)
    if guard_status == 0:
        assert business.read_text() == 'slurm-mapped'
