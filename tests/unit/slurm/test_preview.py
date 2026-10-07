import pytest

from hpc_mcp.slurm.preview import create_submission_preview


VALID_SCRIPT = """#!/bin/bash
#SBATCH --job-name=demo
#SBATCH --partition=gpu
#SBATCH --output=logs/%j.out
#SBATCH --error=logs/%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
python3 main.py
"""


def test_preview_is_non_executing_and_contains_structured_summary() -> None:
    preview = create_submission_preview("jobs/demo.sbatch", VALID_SCRIPT)

    assert preview.script_path == "jobs/demo.sbatch"
    assert preview.resources.job_name == "demo"
    assert preview.entry_command == "python3 main.py"
    assert preview.warnings == []
    assert preview.executable is False


@pytest.mark.parametrize(
    ("path", "script", "message"),
    [
        ("../outside.sbatch", VALID_SCRIPT, "outside the remote project"),
        ("bad\x00name.sbatch", VALID_SCRIPT, "NUL or newline"),
        ("jobs/bad\nname.sbatch", VALID_SCRIPT, "NUL or newline"),
        ("jobs/no-shebang.sbatch", "echo hi\n", "shebang"),
        ("jobs/bad-cpu.sbatch", "#!/bin/bash\n#SBATCH --cpus-per-task=0\necho hi\n", "cpus-per-task"),
        ("jobs/bad-gpu.sbatch", "#!/bin/bash\n#SBATCH --gres=gpu:zero\necho hi\n", "gpu"),
    ],
)
def test_preview_rejects_unsafe_or_invalid_input(path: str, script: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        create_submission_preview(path, script)


def test_preview_rejects_scripts_larger_than_one_mebibyte() -> None:
    script = "#!/bin/bash\n" + "#" * (1024 * 1024)

    with pytest.raises(ValueError, match="1 MiB"):
        create_submission_preview("jobs/large.sbatch", script)


def test_preview_accepts_hour_form_time_limits_beyond_twenty_nine_hours() -> None:
    script = "#!/bin/bash\n#SBATCH --time=48:00:00\necho hi\n"

    preview = create_submission_preview("jobs/long.sbatch", script)

    assert preview.resources.time == "48:00:00"


def test_preview_rejects_day_qualified_time_with_hour_above_twenty_three() -> None:
    script = "#!/bin/bash\n#SBATCH --time=1-29:00:00\necho hi\n"

    with pytest.raises(ValueError, match="--time"):
        create_submission_preview("jobs/invalid-day-hour.sbatch", script)


def test_preview_warns_for_each_missing_optional_submission_detail() -> None:
    preview = create_submission_preview("jobs/minimal.sbatch", "#!/bin/bash\n# comment only\n")

    assert preview.warnings == [
        "No partition requested.",
        "No output log path requested.",
        "No error log path requested.",
        "No time limit requested.",
        "No memory request specified.",
        "No entry command found.",
    ]
