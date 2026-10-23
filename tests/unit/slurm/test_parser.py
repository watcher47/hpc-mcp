import pytest

from hpc_mcp.slurm.parser import parse_sbatch_directives, parse_squeue_rows


def test_counts_all_typed_gpu_entries():
    result = parse_sbatch_directives('#SBATCH --gres=gpu:card_a:1,gpu:card_b:2')
    assert result.gpus == 3
    assert result.gpu_gres == {'card_a': 1, 'card_b': 2}


@pytest.mark.parametrize('value', ['', 'gpu:card_a:1,gpu:card_b:0', 'gpu:bad:-1', 'gpu:bad:x', 'gpu:a:b:1', 'gpu'])
def test_malformed_gpu_request_fails_preview(value):
    from hpc_mcp.slurm.preview import create_submission_preview
    with pytest.raises(ValueError, match='gpu'):
        create_submission_preview('job.sbatch', '#!/bin/bash\n#SBATCH --gres='+value+'\necho ok\n')


def test_missing_gres_value_fails_preview():
    from hpc_mcp.slurm.preview import create_submission_preview
    with pytest.raises(ValueError, match='gpu'):
        create_submission_preview('job.sbatch', '#!/bin/bash\n#SBATCH --gres\necho ok\n')


def test_parse_sbatch_directives() -> None:
    script = """#!/bin/bash
#SBATCH --job-name=demo
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00
python3 main.py
"""
    result = parse_sbatch_directives(script)

    assert result.job_name == "demo"
    assert result.partition == "gpu"
    assert result.gpus == 1
    assert result.cpus_per_task == 4
    assert result.memory == "16G"
    assert result.time == "02:00:00"
    assert result.entry_command == "python3 main.py"


def test_parse_space_delimited_directives_and_keeps_unknowns() -> None:
    result = parse_sbatch_directives(
        "#!/usr/bin/env bash\n"
        "#SBATCH --job-name demo\n"
        "#SBATCH --output logs/%j.out\n"
        "#SBATCH --mail-type END\n"
        "echo hello\n"
    )

    assert result.job_name == "demo"
    assert result.output == "logs/%j.out"
    assert result.extra_directives == {"--mail-type": "END"}
    assert result.entry_command == "echo hello"


def test_parse_squeue_delimited_rows() -> None:
    rows = parse_squeue_rows("123|RUNNING|demo|gpu|00:01:02|node01\n")

    assert rows[0].job_id == "123"
    assert rows[0].state == "RUNNING"
    assert rows[0].name == "demo"
    assert rows[0].partition == "gpu"
    assert rows[0].elapsed == "00:01:02"
    assert rows[0].node_list == "node01"


def test_parse_squeue_rejects_rows_without_the_fixed_field_count() -> None:
    with pytest.raises(ValueError, match="six pipe-delimited fields"):
        parse_squeue_rows("123 RUNNING demo gpu 00:01:02 node01\n")
