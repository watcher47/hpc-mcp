"""Validated GPU selection input; physical identity is resolved by the probe."""
from typing import Literal
import re
import json
from pathlib import Path
from pydantic import BaseModel, ConfigDict, StrictInt, model_validator


class GpuSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    mode: Literal['exact', 'auto']
    indices: list[StrictInt] | None = None
    uuids: list[str] | None = None
    gres_types: list[str] | None = None
    count: StrictInt | None = None

    @model_validator(mode='after')
    def validate_selection(self):
        selectors = [self.indices, self.uuids, self.gres_types]
        if self.mode == 'auto':
            if self.count is None or self.count <= 0 or any(x is not None for x in selectors):
                raise ValueError('auto requires positive count and no selectors')
        elif self.count is not None or sum(x is not None for x in selectors) != 1 or not any(selectors):
            raise ValueError('exact requires exactly one nonempty selector and no count')
        for name in ('indices', 'uuids', 'gres_types'):
            values = getattr(self, name)
            if values is None:
                continue
            if len(values) != len(set(values)):
                raise ValueError('duplicate GPU selector')
            for value in values:
                valid = value >= 0 if name == 'indices' else bool(re.fullmatch('GPU-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*' if name == 'uuids' else '[A-Za-z0-9_][A-Za-z0-9_.-]*', value))
                if not valid:
                    raise ValueError('invalid GPU selector identifier')
            object.__setattr__(self, name, sorted(values))
        return self


class GpuService:
    """Read-only probe execution; no deployed files or remote metadata writes."""

    def __init__(self, registry, client):
        self._registry, self._client = registry, client

    async def inventory(self, project_id: str):
        return await self._probe(project_id)

    async def preflight(self, project_id: str, selection: GpuSelection):
        return await self._probe(project_id, selection)

    async def _probe(self, project_id, selection=None):
        from hpc_mcp.errors import HpcMcpError
        from hpc_mcp.project.paths import RemotePathGuard
        from hpc_mcp.remote.files import _project_record
        from hpc_mcp.slurm import resource_probe
        from hpc_mcp.slurm.gpu_submission import validate_plan
        from hpc_mcp.ssh.policy import RemoteCommand
        record = await _project_record(self._registry, project_id)
        root = await RemotePathGuard.resolve(self._client, record, '.')
        args = ['-I', '-', 'inventory']
        if selection is not None:
            selection = GpuSelection.model_validate(selection)
            args = ['-I', '-', 'preflight', '--selection-json', json.dumps(selection.model_dump(exclude_none=True), sort_keys=True)]
        result = await self._client.run(RemoteCommand('python3', args, root, timeout_seconds=600, standard_input=Path(resource_probe.__file__).read_bytes()))
        try:
            if result.truncated or len(result.stdout.encode()) > 2 * 1024 * 1024:
                raise ValueError('Probe output exceeds limit')
            data = json.loads(result.stdout)
            if not isinstance(data, dict) or data.get('status') not in {'ready','queueable','busy','unknown'} or not isinstance(data.get('reasons'), list):
                raise ValueError('Invalid GPU probe response')
            expected_exit = 75 if data['status'] == 'busy' else 76 if data['status'] == 'unknown' else 0
            if result.exit_status != expected_exit:
                raise ValueError('GPU probe status disagrees with exit code')
            allowed = data['status'] in {'ready', 'queueable'}
            if allowed and data.get('admission', {}).get('allowed') is not True:
                raise ValueError('Missing GPU admission evidence')
            if allowed and selection is not None:
                validate_plan(data['selection'])
            if not allowed and (data.get('selection') is not None or data.get('admission', {}).get('allowed', False) is not False):
                raise ValueError('Rejected probe contains an allowed plan')
            data.setdefault('admission', {'allowed':False})
            data.setdefault('selection', None)
            return data
        except (ValueError, KeyError, TypeError, AttributeError):
            raise HpcMcpError('GPU_UNKNOWN', 'GPU probe output could not be verified') from None
