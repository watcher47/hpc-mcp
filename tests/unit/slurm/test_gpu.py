import pytest
from pydantic import ValidationError
from hpc_mcp.slurm.gpu import GpuSelection


def test_selectors_are_sorted():
    assert GpuSelection(mode='exact', indices=[3, 0]).indices == [0, 3]
    assert GpuSelection(mode='exact', uuids=['GPU-b', 'GPU-a']).uuids == ['GPU-a', 'GPU-b']
    assert GpuSelection(mode='exact', gres_types=['card_b', 'card_a']).gres_types == ['card_a', 'card_b']
    assert GpuSelection(mode='auto', count=2).model_dump(exclude_none=True) == {'mode': 'auto', 'count': 2}


@pytest.mark.parametrize('data', [
    {'mode':'exact'}, {'mode':'exact','indices':[]},
    {'mode':'exact','indices':[0,0]}, {'mode':'exact','indices':[-1]},
    {'mode':'exact','indices':[True]}, {'mode':'exact','indices':['1']},
    {'mode':'exact','indices':[0],'count':1},
    {'mode':'exact','indices':[0],'uuids':['GPU-a']},
    {'mode':'exact','uuids':['GPU-a','GPU-a']},
    {'mode':'exact','uuids':['MIG-a']},
    {'mode':'exact','gres_types':['card;id']},
    {'mode':'auto','count':0}, {'mode':'auto','count':True},
    {'mode':'auto','count':1,'indices':[0]}, {'mode':'auto','count':1,'extra':1},
])
def test_rejects_invalid_selection(data):
    with pytest.raises(ValidationError):
        GpuSelection(**data)
