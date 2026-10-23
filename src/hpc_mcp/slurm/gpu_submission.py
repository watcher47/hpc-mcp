"""Standalone intent validation and Bash wrapper generation for selected GPUs."""
from __future__ import annotations

import re
import shlex

IDENTIFIER = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]*')
GPU_UUID = re.compile(r'GPU-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*')


def canonical_selector(value):
    if not isinstance(value, dict):
        raise ValueError('Invalid GPU selector')
    value = {key: item for key, item in value.items() if item is not None}
    mode = value.get('mode')
    if mode == 'auto':
        if set(value) != {'mode', 'count'} or type(value['count']) is not int or value['count'] <= 0:
            raise ValueError('auto requires a positive count')
        return value
    fields = set(value) - {'mode'}
    if mode != 'exact' or len(fields) != 1 or not fields <= {'indices', 'uuids', 'gres_types'}:
        raise ValueError('exact requires one selector')
    name = fields.pop()
    items = value[name]
    if not isinstance(items, list) or not items:
        raise ValueError('GPU selector must be a nonempty list')
    for item in items:
        valid = type(item) is int and item >= 0 if name == 'indices' else isinstance(item, str) and (GPU_UUID if name == 'uuids' else IDENTIFIER).fullmatch(item)
        if not valid:
            raise ValueError('Invalid GPU identifier')
    if len(set(items)) != len(items):
        raise ValueError('Duplicate GPU identifier')
    return {'mode': mode, name: sorted(items)}


def validate_plan(plan):
    if not isinstance(plan, dict) or set(plan) != {'node', 'gpus', 'gres'}:
        raise ValueError('Invalid GPU plan')
    if not isinstance(plan['node'], str) or not IDENTIFIER.fullmatch(plan['node']):
        raise ValueError('Invalid selected node')
    cards = plan['gpus']
    if not isinstance(cards, list) or not cards:
        raise ValueError('Missing physical GPU identities')
    for card in cards:
        if not isinstance(card, dict) or set(card) != {'index', 'uuid', 'minor', 'gres_type'}:
            raise ValueError('Invalid physical GPU identity')
        if any(type(card[k]) is not int or card[k] < 0 for k in ('index', 'minor')) or not isinstance(card['uuid'], str) or not GPU_UUID.fullmatch(card['uuid']) or not isinstance(card['gres_type'], str) or not IDENTIFIER.fullmatch(card['gres_type']):
            raise ValueError('Invalid physical GPU identity')
    if any(len({card[k] for card in cards}) != len(cards) for k in ('index', 'uuid', 'minor', 'gres_type')):
        raise ValueError('Ambiguous physical GPU identities')
    expected = ','.join('gpu:'+name+':1' for name in sorted(card['gres_type'] for card in cards))
    if plan['gres'] != expected:
        raise ValueError('Invalid canonical GPU GRES')
    return plan


_SHORT = {'-J':'--job-name', '-p':'--partition', '-c':'--cpus-per-task', '-t':'--time', '-o':'--output', '-e':'--error', '-N':'--nodes', '-n':'--ntasks', '-w':'--nodelist', '-A':'--account', '-q':'--qos', '-C':'--constraint'}
_VALUE_OPTIONS = {'--job-name', '--partition', '--cpus-per-task', '--time', '--output', '--error', '--nodes', '--ntasks', '--nodelist', '--gres', '--mem', '--mem-per-cpu', '--account', '--qos', '--constraint', '--mail-type', '--mail-user', '--dependency', '--begin', '--exclude', '--ntasks-per-node', '--ntasks-per-core', '--hint', '--distribution', '--export', '--signal', '--nice', '--reservation', '--comment', '--open-mode'}
_FLAG_OPTIONS = {'--exclusive', '--requeue', '--no-requeue', '--oversubscribe', '--hold'}


def script_header(source, plan=None):
    """Parse only the scheduler-effective initial header; reject unsafe forms."""
    lines = source.splitlines(keepends=True)
    if not lines or lines[0].rstrip('\r\n') not in ('#!/bin/bash', '#!/usr/bin/bash', '#!/usr/bin/env bash'):
        raise ValueError('Selected GPUs require a plain Bash batch script')
    split = len(lines)
    options = {}
    for index, line in enumerate(lines[1:], 1):
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            split = index
            break
        if not stripped.startswith('#SBATCH'):
            continue
        if not re.match(r'#SBATCH\s+', stripped):
            raise ValueError('Unsupported SBATCH directive')
        tokens = shlex.split(stripped[7:].strip())
        if not tokens:
            raise ValueError('Empty SBATCH directive')
        key, equals, value = tokens[0].partition('=')
        if equals:
            if len(tokens) != 1:
                raise ValueError('Use one SBATCH option per line')
        else:
            if len(tokens) > 2:
                raise ValueError('Use one SBATCH option per line')
            value = tokens[1] if len(tokens) == 2 else ''
        key = _SHORT.get(key, key)
        if key not in _VALUE_OPTIONS | _FLAG_OPTIONS or (key in _VALUE_OPTIONS and not value) or (key in _FLAG_OPTIONS and value):
            raise ValueError('Remove unsupported SBATCH option: '+key)
        if key in options:
            raise ValueError('Duplicate SBATCH options are unsupported for selected GPUs')
        options[key] = value
    if any(line.lstrip().startswith('#SBATCH') for line in lines[split:]):
        raise ValueError('Move all SBATCH directives to the initial comment header')
    for key, value in options.items():
        if key == '--nodes' and value != '1':
            raise ValueError('Selected GPUs require exactly one node')
        if key == '--gres':
            if any(not re.fullmatch(r'gpu:[A-Za-z0-9_][A-Za-z0-9_.-]*:[1-9]\d*', part) for part in value.split(',')):
                raise ValueError('Remove or update competing GPU/non-GPU GRES directives')
            if plan is not None and sorted(value.split(',')) != sorted(plan['gres'].split(',')):
                raise ValueError('Remove or update the competing GPU GRES directive')
        if plan is not None and key == '--nodelist' and value != plan['node']:
            raise ValueError('Remove or update the competing node directive')
    return ''.join(lines[:split]), ''.join(lines[split:]), options


def wrapper(source, root, probe_path, probe_sha256, expected_path, report_path):
    header, body, _ = script_header(source)
    if not header.endswith('\n'):
        header += '\n'
    # Execute the verified bytes rather than reading the helper a second time.
    # The probe and this loader import only the standard library under -I.
    verify = """import hashlib, pathlib, stat, sys
try:
    p = pathlib.Path(sys.argv[1])
    root = pathlib.Path(sys.argv[3]).resolve(strict=True)
    if p.is_symlink() or not p.resolve(strict=True).is_relative_to(root) or not stat.S_ISREG(p.stat().st_mode):
        sys.exit(76)
    data = p.read_bytes()
    if hashlib.sha256(data).hexdigest() != sys.argv[2]:
        sys.exit(76)
except (OSError, ValueError):
    sys.exit(76)
sys.argv = [str(p), *sys.argv[4:]]
exec(compile(data, str(p), 'exec'), {'__name__': '__main__', '__file__': str(p)})
"""
    prefix, suffix = str(report_path).split('${SLURM_JOB_ID}')
    report = shlex.quote(prefix) + '"${SLURM_JOB_ID}"' + shlex.quote(suffix)
    command = ' '.join(shlex.quote(str(item)) for item in ('python3', '-I', '-', probe_path, probe_sha256, root, 'guard', '--expected-json', expected_path))
    return header + 'export CUDA_DEVICE_ORDER=PCI_BUS_ID\nif ' + command + ' --output ' + report + " <<'HPC_GPU_VERIFY'\n" + verify + 'HPC_GPU_VERIFY\nthen\n    :\nelse\n    exit $?\nfi\n' + body
