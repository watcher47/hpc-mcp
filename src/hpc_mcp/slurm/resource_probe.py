#!/usr/bin/env python3
"""Standalone, read-only whole NVIDIA GPU identity and occupancy probe.

Snapshot schema: node (uniquely resolved local Slurm node), node_details (scontrol
line), gres (two sinfo columns), gres_conf, nvidia_q (raw text), gpu_samples
(three lists of index/uuid/memory_used_mib/utilization_percent/has_compute_app
dicts), compute_apps (union of gpu_uuid/pid/slurm_owned observations), sampled_at.
No Slurm IDX value is interpreted as a NVIDIA index. Mapping/parsing follows
the slurm-run resource probe through device minor -> UUID -> current index.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

BUSY_EXIT = 75
UNKNOWN_EXIT = 76
GPU_QUERY = 'index,uuid,memory.used,utilization.gpu,name'
APP_QUERY = 'gpu_uuid,pid'
IDENTIFIER = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]*\Z')
UUID = re.compile(r'GPU-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*\Z')


def _fields(text):
    return dict(re.findall(r'(?:^|\s)([A-Za-z][A-Za-z0-9_./]*)=(.*?)(?=\s+[A-Za-z][A-Za-z0-9_./]*=|$)', text.strip()))


def _reason(code, message):
    return {'code': code, 'message': message}


def _indices(spec):
    result = set()
    for token in spec.split(','):
        if not re.fullmatch(r'\d+(?:-\d+)?', token):
            raise ValueError('invalid Slurm GPU index range')
        bounds = [int(x) for x in token.split('-')]
        first, last = bounds[0], bounds[-1]
        if last < first or last - first > 1024:
            raise ValueError('invalid Slurm GPU index range')
        for value in range(first, last + 1):
            if value in result:
                raise ValueError('duplicate Slurm GPU index')
            result.add(value)
    return sorted(result)


def _gres_entries(column):
    if column in {'(null)', 'N/A', ''}:
        return {}
    result = {}
    # Commas within IDX belong to the current entry.
    for token in re.split(r',(?=[^()]*?(?:\(|$))', column):
        if not token.startswith('gpu'):
            continue
        match = re.fullmatch(r'gpu(?::([A-Za-z0-9_][A-Za-z0-9_.-]*))?:(\d+)(?:\((?:IDX:([0-9,-]*|N/A)|S:[0-9,-]+)\))?', token)
        if not match:
            raise ValueError('invalid or truncated GPU GRES field')
        gpu_type, count, spec = match.groups()
        key = gpu_type or ''
        if key in result:
            raise ValueError('duplicate GPU GRES type')
        count = int(count)
        indices = _indices(spec) if spec and spec != 'N/A' else []
        if indices and len(indices) != count:
            raise ValueError('inconsistent GPU GRES indices')
        result[key] = (count, indices)
    return result


def parse_gres_usage(text):
    columns = text.strip().split()
    if len(columns) != 2:
        raise ValueError('expected complete Gres and GresUsed columns')
    configured, used = map(_gres_entries, columns)
    if not configured or not set(used) <= set(configured):
        raise ValueError('Gres and GresUsed types disagree')
    if columns[1] not in {'(null)', 'N/A'} and set(used) != set(configured):
        raise ValueError('GPU allocation evidence missing for configured types')
    types = {}
    for name, (total, _) in configured.items():
        count, indices = used.get(name, (0, []))
        if count > total:
            raise ValueError('allocated GPU count exceeds configured count')
        types[name] = dict(total=total, used_count=count, used_indices=indices)
    return dict(types=types, total=sum(x['total'] for x in types.values()), used_count=sum(x['used_count'] for x in types.values()))


def parse_gpu_device_map(gres_conf, nvidia_q, gpu_rows, node_name):
    type_minors = {}
    defaults = None
    for line in gres_conf.splitlines():
        fields = _fields(line.split('#', 1)[0])
        if fields.get('NodeName') == 'DEFAULT':
            defaults = fields
            continue
        if fields.get('Name') != 'gpu':
            continue
        if defaults:
            fields = {**defaults, **fields}
        node = fields.get('NodeName')
        if node is not None and node != node_name:
            # Hostlist ranges are intentionally unsupported rather than guessed.
            continue
        name = fields.get('Type', '')
        file_match = re.fullmatch(r'/dev/nvidia(\d+)', fields.get('File', ''))
        if not IDENTIFIER.fullmatch(name) or not file_match or name in type_minors or fields.get('Count', '1') != '1' or 'MultipleFiles' in fields:
            raise ValueError('GPU configuration does not identify one whole card per type')
        type_minors[name] = int(file_match.group(1))
    if not type_minors or len(set(type_minors.values())) != len(type_minors):
        raise ValueError('missing or duplicate GPU device files')
    minors = {}
    blocks = re.split(r'^GPU\s+[0-9A-Fa-f]+:[0-9A-Fa-f]+:[0-9A-Fa-f]+\.[0-9]+.*$', nvidia_q, flags=re.M)[1:]
    for block in blocks:
        uuid = re.search(r'GPU UUID\s*:\s*(GPU-[A-Za-z0-9-]+)', block)
        minor = re.search(r'Minor Number\s*:\s*(\d+)', block)
        if not uuid or not minor or int(minor[1]) in minors or uuid[1] in minors.values():
            raise ValueError('incomplete or duplicate NVIDIA minor mapping')
        if re.search(r'MIG Mode\s*\n\s*Current\s*:\s*Enabled', block):
            raise ValueError('MIG GPUs unsupported')
        minors[int(minor[1])] = uuid[1]
    by_uuid = {row['uuid']: row['index'] for row in gpu_rows}
    if len(by_uuid) != len(gpu_rows) or len(set(by_uuid.values())) != len(by_uuid) or set(type_minors.values()) != set(minors) or set(minors.values()) != set(by_uuid):
        raise ValueError('Slurm device files disagree with NVIDIA identities')
    return {name: dict(index=by_uuid[minors[minor]], uuid=minors[minor], minor=minor, gres_type=name) for name, minor in type_minors.items()}


def parse_gpu_rows(text):
    rows = []
    for parts in csv.reader(text.splitlines(), skipinitialspace=True):
        if not parts:
            continue
        if len(parts) not in (4, 5):
            raise ValueError('invalid NVIDIA metrics row')
        index, uuid, memory, utilization = parts[:4]
        row = dict(index=int(index), uuid=uuid.strip(), memory_used_mib=int(memory), utilization_percent=int(utilization))
        if row['index'] < 0 or not UUID.fullmatch(row['uuid']) or row['memory_used_mib'] < 0 or not 0 <= row['utilization_percent'] <= 100:
            raise ValueError('invalid NVIDIA metrics')
        if len(parts) == 5:
            row['name'] = parts[4].strip()
        rows.append(row)
    if not rows or len({x['index'] for x in rows}) != len(rows) or len({x['uuid'] for x in rows}) != len(rows):
        raise ValueError('missing or duplicate NVIDIA metrics')
    return rows


def _selection(selection):
    if not isinstance(selection, dict) or set(selection) - {'mode','count','indices','uuids','gres_types'}:
        raise ValueError('invalid GPU selection')
    selectors = [key for key in ('indices','uuids','gres_types') if selection.get(key) is not None]
    if selection.get('mode') == 'auto':
        if selectors or type(selection.get('count')) is not int or selection['count'] <= 0:
            raise ValueError('auto requires positive count and no selectors')
    elif selection.get('mode') == 'exact':
        if len(selectors) != 1 or selection.get('count') is not None:
            raise ValueError('exact requires one selector and no count')
        key = selectors[0]
        values = selection[key]
        if not isinstance(values, list) or not values:
            raise ValueError('empty selector')
        for value in values:
            if key == 'indices':
                if type(value) is not int or value < 0:
                    raise ValueError('invalid GPU index')
            elif not isinstance(value, str) or not (UUID if key == 'uuids' else IDENTIFIER).fullmatch(value):
                raise ValueError('invalid GPU identifier')
        if len(set(values)) != len(values):
            raise ValueError('duplicate GPU selector')
    else:
        raise ValueError('invalid GPU selection mode')
    return selection


def _evidence(snapshot):
    if snapshot.get('errors'):
        raise ValueError('resource collection incomplete')
    node = snapshot['node']
    if not isinstance(node, str) or not IDENTIFIER.fullmatch(node):
        raise ValueError('local Slurm node unknown')
    fields = _fields(snapshot['node_details'])
    if fields.get('NodeName') != node:
        raise ValueError('Slurm node identity mismatch')
    samples = snapshot['gpu_samples']
    if not isinstance(samples, list) or len(samples) != 3 or not samples[0]:
        raise ValueError('three GPU samples required')
    first = samples[0]
    identities = {(row['index'], row['uuid']) for row in first}
    if len(identities) != len(first) or len({x[0] for x in identities}) != len(first) or len({x[1] for x in identities}) != len(first):
        raise ValueError('duplicate GPU identity')
    for sample in samples:
        if len(sample) != len(first) or {(row['index'], row['uuid']) for row in sample} != identities:
            raise ValueError('GPU identity changed during sampling')
        for row in sample:
            if type(row['index']) is not int or row['index'] < 0 or not UUID.fullmatch(row['uuid']):
                raise ValueError('invalid GPU identity')
            if type(row['memory_used_mib']) is not int or row['memory_used_mib'] < 0 or type(row['utilization_percent']) is not int or not 0 <= row['utilization_percent'] <= 100 or type(row['has_compute_app']) is not bool:
                raise ValueError('GPU metric or process observation missing')
    usage = parse_gres_usage(snapshot['gres'])['types']
    devices = parse_gpu_device_map(snapshot['gres_conf'], snapshot['nvidia_q'], first, node)
    if set(usage) != set(devices) or any(v['total'] != 1 for v in usage.values()):
        raise ValueError('GPU GRES types do not identify exactly one card')
    apps = snapshot['compute_apps']
    if not isinstance(apps, list):
        raise ValueError('compute process evidence missing')
    for app in apps:
        if app['gpu_uuid'] not in {x[1] for x in identities} or type(app['pid']) is not int or app['pid'] <= 0 or type(app['slurm_owned']) is not bool:
            raise ValueError('compute process attribution unknown')
    for sample in samples:
        for row in sample:
            if row['has_compute_app'] and not any(a['gpu_uuid'] == row['uuid'] for a in apps):
                raise ValueError('compute process attribution missing')
    return node, fields, samples, usage, devices, apps


def evaluate_snapshot(snapshot: dict, selection: dict | None = None) -> dict:
    result = dict(status='unknown', admission={'allowed':False}, node=snapshot.get('node'), gpus=[], selection=None, reasons=[], sampled_at=snapshot.get('sampled_at'))
    # Preserve observed identities for inventory even if the precise Slurm map
    # cannot be proven. These rows never advertise selectable resources.
    try:
        for row in snapshot['gpu_samples'][0]:
            if type(row['index']) is int and row['index'] >= 0 and UUID.fullmatch(row['uuid']):
                result['gpus'].append(dict(index=row['index'],uuid=row['uuid'],name=row.get('name'),minor=None,gres_type=None,slurm_allocated=None,externally_occupied=None,selectable=False))
    except (KeyError, TypeError, IndexError):
        pass
    try:
        node, fields, samples, usage, devices, apps = _evidence(snapshot)
        rows = []
        for name, device in sorted(devices.items()):
            related = [a for a in apps if a['gpu_uuid'] == device['uuid']]
            observed = [r for sample in samples for r in sample if r['uuid'] == device['uuid']]
            external = any(a['slurm_owned'] is False for a in related)
            held = usage[name]['used_count'] == 1
            idle = not related and all(not r['has_compute_app'] and r['memory_used_mib'] <= 512 and r['utilization_percent'] <= 5 for r in observed)
            rows.append({**device, 'name': observed[0].get('name'), 'slurm_allocated':held, 'externally_occupied':external, 'selectable':not external and (held or idle)})
        result['gpus'] = rows
        if fields.get('State', '').split('+')[0] not in {'IDLE','MIXED','ALLOCATED'} or any(x in fields.get('State','') for x in ('DRAIN','DOWN','FAIL','NO_RESPOND')):
            result.update(status='busy', reasons=[_reason('node_unavailable', 'Slurm node is unavailable')])
            return result
        if selection is None:
            result.update(status='ready', admission={'allowed':True})
            return result
        _selection(selection)
        if selection['mode'] == 'auto':
            chosen = sorted((r for r in rows if r['selectable']), key=lambda r:(r['slurm_allocated'],r['gres_type']))[:selection['count']]
            wanted = selection['count']
        else:
            key = next(k for k in ('indices','uuids','gres_types') if selection.get(k) is not None)
            field = {'indices':'index','uuids':'uuid','gres_types':'gres_type'}[key]
            chosen = [r for r in rows if r[field] in selection[key]]
            wanted = len(selection[key])
        if len(chosen) != wanted or any(not r['selectable'] for r in chosen):
            result.update(status='busy', reasons=[_reason('gpu_unavailable','Requested GPU selection unavailable')])
            return result
        chosen = sorted(chosen, key=lambda r:r['gres_type'])
        result.update(status='queueable' if any(r['slurm_allocated'] for r in chosen) else 'ready', admission={'allowed':True}, selection=dict(node=node, gpus=[{k:r[k] for k in ('index','uuid','minor','gres_type')} for r in chosen], gres=','.join('gpu:'+r['gres_type']+':1' for r in chosen)))
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        result.update(status='unknown', admission={'allowed':False}, selection=None, reasons=[_reason('gpu_evidence_unknown','GPU identity, allocation or observation evidence is incomplete or unsupported')])
    return result


def evaluate_job_guard(expected: dict, job_details: str, snapshot: dict) -> dict:
    result = dict(status='unknown', reasons=[], allocated_gpus=[], sampled_at=snapshot.get('sampled_at'))
    try:
        node, _, samples, _, devices, apps = _evidence(snapshot)
        fields = _fields(job_details)
        if expected['node'] != node or fields.get('NodeList') != node:
            raise ValueError('allocated node differs from expected')
        entries = _gres_entries(fields.get('GresDetail') or fields.get('GRES',''))
        actual = {name for name,(count,indices) in entries.items() if count}
        if not actual or any(count not in (0,1) or (count == 1 and len(indices) != 1) for count,indices in entries.values()):
            raise ValueError('exact allocated GRES detail missing')
        wanted = expected['gpus']
        if len(wanted) != len({r['gres_type'] for r in wanted}) or actual != {r['gres_type'] for r in wanted}:
            raise ValueError('allocated GRES types differ')
        for row in wanted:
            current = devices[row['gres_type']]
            if any(current[k] != row[k] for k in ('uuid','minor','gres_type')):
                raise ValueError('allocated GPU identity remapped')
        result['allocated_gpus'] = [devices[name] for name in sorted(actual)]
        uuids = {r['uuid'] for r in result['allocated_gpus']}
        busy = any(a['gpu_uuid'] in uuids for a in apps) or any(r['has_compute_app'] or r['memory_used_mib'] > 512 or r['utilization_percent'] > 5 for sample in samples for r in sample if r['uuid'] in uuids)
        result.update(status='busy' if busy else 'idle', reasons=[_reason('allocated_gpu_busy','Allocated GPU is occupied')] if busy else [])
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        result['reasons'] = [_reason('allocation_unknown','Allocated GPU identity or observations cannot be verified')]
    return result


def _command(argv):
    completed = subprocess.run(argv, check=True, capture_output=True, text=True, timeout=20)
    if len(completed.stdout) > 2 * 1024 * 1024:
        raise ValueError('probe output exceeds limit')
    return completed.stdout


def _slurm_owned_pid(pid):
    result = subprocess.run(['scontrol','pidinfo',str(pid)], check=False, capture_output=True, text=True, timeout=20)
    if result.returncode == 0 and re.search(r'\b(?:JobId[= ]+|Slurm job ID\s+)\d+', result.stdout, re.I):
        return True
    if 'Failed to locate job for requested pid' in result.stdout + result.stderr:
        return False
    raise ValueError('Slurm process attribution unavailable')


def collect_snapshot() -> dict:
    snapshot = {'sampled_at':datetime.now(timezone.utc).isoformat()}
    try:
        hostnames = {socket.gethostname(), socket.getfqdn()}
        hostnames |= {h.split('.')[0] for h in hostnames}
        nodes = _command(['scontrol','show','nodes','-o']).splitlines()
        matches = [line for line in nodes if any(_fields(line).get(k) in hostnames for k in ('NodeName','NodeHostName','NodeAddr'))]
        if len(matches) != 1:
            raise ValueError('local host does not uniquely map to Slurm node')
        node = _fields(matches[0])['NodeName']
        snapshot.update(node=node, node_details=matches[0], gres=_command(['sinfo','-N','-h','-n',node,'-O','Gres:10000,GresUsed:10000']), gres_conf=Path('/etc/slurm/gres.conf').read_text(encoding='utf-8'), nvidia_q=_command(['nvidia-smi','-q']))
        samples, apps = [], {}
        for i in range(3):
            rows = parse_gpu_rows(_command(['nvidia-smi','--query-gpu='+GPU_QUERY,'--format=csv,noheader,nounits']))
            active = []
            for parts in csv.reader(_command(['nvidia-smi','--query-compute-apps='+APP_QUERY,'--format=csv,noheader,nounits']).splitlines(), skipinitialspace=True):
                if not parts:
                    continue
                if len(parts) != 2 or not UUID.fullmatch(parts[0].strip()) or not parts[1].strip().isdigit():
                    raise ValueError('invalid compute process evidence')
                uuid, pid = parts[0].strip(), int(parts[1])
                key = uuid, pid
                if key not in apps:
                    apps[key] = dict(gpu_uuid=uuid,pid=pid,slurm_owned=_slurm_owned_pid(pid))
                active.append(uuid)
            for row in rows:
                row['has_compute_app'] = row['uuid'] in active
            samples.append(rows)
            if i < 2:
                time.sleep(1)
        snapshot.update(gpu_samples=samples, compute_apps=list(apps.values()))
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        snapshot['errors'] = ['resource collection failed']
    return snapshot


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='mode',required=True)
    commands.add_parser('inventory')
    preflight = commands.add_parser('preflight')
    preflight.add_argument('--selection-json',required=True)
    guard = commands.add_parser('guard')
    guard.add_argument('--expected-json',required=True)
    guard.add_argument('--output',required=True)
    args = parser.parse_args(argv)
    try:
        if args.mode == 'guard':
            expected = json.loads(Path(args.expected_json).read_text(encoding='utf-8'))
            job_id = os.environ.get('SLURM_JOB_ID','')
            if not job_id.isdigit():
                raise ValueError('SLURM_JOB_ID missing')
            job = _command(['scontrol','show','job','-dd',job_id])
            result = evaluate_job_guard(expected,job,collect_snapshot())
            result['job_id'] = job_id
        else:
            selection = json.loads(args.selection_json) if args.mode == 'preflight' else None
            result = evaluate_snapshot(collect_snapshot(),selection)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        result = dict(status='unknown',reasons=[_reason('probe_failed','Probe input or resource evidence unavailable')])
    payload = json.dumps(result,sort_keys=True) + '\n'
    if args.mode == 'guard':
        try:
            with Path(args.output).open('x',encoding='utf-8') as stream:
                stream.write(payload)
        except OSError:
            result = dict(status='unknown',reasons=[_reason('report_failed','Cannot create guard report without overwriting')])
            payload = json.dumps(result,sort_keys=True) + '\n'
    sys.stdout.write(payload)
    return 0 if result['status'] in {'ready','queueable','idle'} else BUSY_EXIT if result['status'] == 'busy' else UNKNOWN_EXIT


if __name__ == '__main__':
    raise SystemExit(main())
