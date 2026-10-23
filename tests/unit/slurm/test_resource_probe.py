from copy import deepcopy
import json
import subprocess
import pytest
from hpc_mcp.slurm import resource_probe as probe


def snapshot():
    """Raw node/GRES/config/NVIDIA evidence plus three parsed observation samples."""
    rows = [dict(index=4, uuid='GPU-a', memory_used_mib=0, utilization_percent=0, has_compute_app=False),
            dict(index=0, uuid='GPU-b', memory_used_mib=0, utilization_percent=0, has_compute_app=False)]
    return dict(node='node1', node_details='NodeName=node1 NodeHostName=host1 State=MIXED',
                gres='gpu:card_a:1,gpu:card_b:1 gpu:card_a:0(IDX:),gpu:card_b:0(IDX:)',
                gres_conf='NodeName=node1 Name=gpu Type=card_a File=/dev/nvidia2\nNodeName=node1 Name=gpu Type=card_b File=/dev/nvidia7',
                nvidia_q='GPU 0000:01:00.0\n GPU UUID : GPU-a\n Minor Number : 2\nGPU 0000:02:00.0\n GPU UUID : GPU-b\n Minor Number : 7',
                gpu_samples=[deepcopy(rows) for _ in range(3)], compute_apps=[], sampled_at='now')


def exact(**kwargs):
    return dict(mode='exact', **kwargs)


@pytest.mark.parametrize('selector', [exact(indices=[4]), exact(uuids=['GPU-a']), exact(gres_types=['card_a'])])
def test_resolves_minor_uuid_current_index(selector):
    result = probe.evaluate_snapshot(snapshot(), selector)
    assert result['status'] == 'ready'
    assert result['selection'] == dict(node='node1', gpus=[dict(index=4, uuid='GPU-a', minor=2, gres_type='card_a')], gres='gpu:card_a:1')


def test_auto_prefers_idle_to_held_and_held_is_queueable():
    data = snapshot()
    data['gres'] = 'gpu:card_a:1,gpu:card_b:1 gpu:card_a:1(IDX:0),gpu:card_b:0(IDX:)'
    data['compute_apps'] = [dict(gpu_uuid='GPU-a', pid=12, slurm_owned=True)]
    assert probe.evaluate_snapshot(data, dict(mode='auto', count=1))['selection']['gres'] == 'gpu:card_b:1'
    assert probe.evaluate_snapshot(data, exact(indices=[4]))['status'] == 'queueable'


def test_external_occupancy_never_substitutes_exact():
    data = snapshot()
    data['compute_apps'] = [dict(gpu_uuid='GPU-a', pid=12, slurm_owned=False)]
    result = probe.evaluate_snapshot(data, exact(indices=[4]))
    assert result['status'] == 'busy'
    assert result['selection'] is None


@pytest.mark.parametrize('change', ['duplicate_minor','missing_attribution','changed_uuid','missing_sample','shared_type'])
def test_ambiguous_or_missing_evidence_unknown(change):
    data = snapshot()
    if change == 'duplicate_minor': data['gres_conf'] = data['gres_conf'].replace('nvidia7','nvidia2')
    if change == 'missing_attribution': data['compute_apps'] = [dict(gpu_uuid='GPU-a', pid=12)]
    if change == 'changed_uuid': data['gpu_samples'][1][0]['uuid'] = 'GPU-c'
    if change == 'missing_sample': data['gpu_samples'].pop()
    if change == 'shared_type': data['gres'] = 'gpu:model:2 gpu:model:0(IDX:)'
    result = probe.evaluate_snapshot(data, exact(indices=[4]))
    assert result['status'] == 'unknown'
    assert not result['admission']['allowed']


def test_idx_commas_and_null_usage():
    assert probe.parse_gres_usage('gpu:model:2 gpu:model:2(IDX:0,1)')['types']['model']['used_count'] == 2
    assert probe.parse_gres_usage('gpu:card_a:1 (null)')['used_count'] == 0


def test_guard_checks_allocated_identity_and_occupancy():
    data = snapshot()
    expected = probe.evaluate_snapshot(data, exact(indices=[4]))['selection']
    job = 'NodeList=node1 GresDetail=gpu:card_a:1(IDX:9)'
    assert probe.evaluate_job_guard(expected, job, data)['status'] == 'idle'
    assert probe.evaluate_job_guard(expected, job.replace('card_a','card_b'), data)['status'] == 'unknown'
    data['gpu_samples'][1][0]['memory_used_mib'] = 900
    assert probe.evaluate_job_guard(expected, job, data)['status'] == 'busy'
    data['gpu_samples'] = []
    assert probe.evaluate_job_guard(expected, job, data)['status'] == 'unknown'


def test_guard_allows_index_shuffle_but_rejects_minor_remap():
    data = snapshot()
    expected = probe.evaluate_snapshot(data, exact(indices=[4]))['selection']
    for sample in data['gpu_samples']: sample[0]['index'] = 8
    job = 'NodeList=node1 GresDetail=gpu:card_a:1(IDX:0)'
    assert probe.evaluate_job_guard(expected, job, data)['status'] == 'idle'
    data['gres_conf'] = data['gres_conf'].replace('Type=card_a','Type=other')
    assert probe.evaluate_job_guard(expected, job, data)['status'] == 'unknown'


def test_inventory_preserves_physical_cards_when_mapping_unknown():
    data = snapshot()
    data['gres'] = 'gpu:model:2 gpu:model:0(IDX:)'
    result = probe.evaluate_snapshot(data)
    assert result['status'] == 'unknown'
    assert len(result['gpus']) == 2
    assert all(r['gres_type'] is None and not r['selectable'] for r in result['gpus'])


def test_partial_usage_is_unknown_instead_of_assuming_free():
    data = snapshot()
    data['gres'] = 'gpu:card_a:1,gpu:card_b:1 gpu:card_a:0(IDX:)'
    assert probe.evaluate_snapshot(data)['status'] == 'unknown'


def boundary(monkeypatch, *, active=False, ambiguous=False, attribution='Slurm job ID 12'):
    data = snapshot()
    def run(argv, **kwargs):
        assert kwargs['timeout'] == 20
        output = ''
        if argv[:3] == ['scontrol','show','nodes']:
            output = data['node_details']
            if ambiguous: output += '\nNodeName=node2 NodeHostName=host1 State=IDLE'
        elif argv[:3] == ['scontrol','show','job']:
            output = 'NodeList=node1 GresDetail=gpu:card_a:1(IDX:0)'
        elif argv[0] == 'sinfo': output = data['gres']
        elif argv == ['nvidia-smi','-q']: output = data['nvidia_q']
        elif argv[0] == 'nvidia-smi' and '--query-gpu=' in argv[1]:
            output = '4, GPU-a, 0, 0, Synthetic GPU\n0, GPU-b, 0, 0, Synthetic GPU'
        elif argv[0] == 'nvidia-smi': output = 'GPU-a, 42' if active else ''
        elif argv[:2] == ['scontrol','pidinfo']: output = attribution
        else: raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=output, stderr='')
    original_read = probe.Path.read_text
    def read(path, *args, **kwargs):
        if str(path).replace('\\','/') == '/etc/slurm/gres.conf': return data['gres_conf']
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(probe.subprocess, 'run', run)
    monkeypatch.setattr(probe.socket, 'gethostname', lambda:'host1')
    monkeypatch.setattr(probe.socket, 'getfqdn', lambda:'host1.example')
    monkeypatch.setattr(probe.Path, 'read_text', read)
    monkeypatch.setattr(probe.time, 'sleep', lambda seconds:None)


def test_collection_attributes_slurm_pid_and_keeps_model(monkeypatch):
    boundary(monkeypatch, active=True)
    data = probe.collect_snapshot()
    assert 'errors' not in data
    assert data['compute_apps'][0]['slurm_owned'] is True
    result = probe.evaluate_snapshot(data, dict(mode='auto',count=1))
    assert result['selection']['gres'] == 'gpu:card_b:1'
    assert result['gpus'][0]['name'] == 'Synthetic GPU'


def test_collection_host_ambiguity_and_missing_attribution_are_unknown(monkeypatch):
    boundary(monkeypatch, ambiguous=True)
    assert probe.evaluate_snapshot(probe.collect_snapshot())['status'] == 'unknown'
    boundary(monkeypatch, active=True, attribution='unrecognized result')
    assert probe.evaluate_snapshot(probe.collect_snapshot())['status'] == 'unknown'


def test_cli_guard_report_is_exclusive_and_checks_current_job(monkeypatch, tmp_path, capsys):
    boundary(monkeypatch)
    monkeypatch.setenv('SLURM_JOB_ID','12')
    expected = tmp_path / 'expected.json'
    expected.write_text(json.dumps(probe.evaluate_snapshot(snapshot(), exact(indices=[4]))['selection']))
    report = tmp_path / 'guard.json'
    args = ['guard','--expected-json',str(expected),'--output',str(report)]
    assert probe.main(args) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'idle'
    original = report.read_bytes()
    assert probe.main(args) == 76
    assert json.loads(capsys.readouterr().out)['status'] == 'unknown'
    assert report.read_bytes() == original


def test_guard_ignores_occupancy_of_unallocated_gpu():
    data = snapshot()
    expected = probe.evaluate_snapshot(data, exact(indices=[4]))['selection']
    for sample in data['gpu_samples']: sample[1]['memory_used_mib'] = 8000
    data['compute_apps'] = [dict(gpu_uuid='GPU-b',pid=12,slurm_owned=False)]
    assert probe.evaluate_job_guard(expected,'NodeList=node1 GresDetail=gpu:card_a:1(IDX:0)',data)['status'] == 'idle'
