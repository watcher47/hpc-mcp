import pytest

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh import policy


def profile(**updates):
    return ClusterConfig(alias="default", host="compute.invalid", username="fixture-user",
                         password="fixture-value", **updates)


def validate(command, **updates):
    return policy.GeneralCommandPolicy(profile(**updates)).validate(command, "/work", None)


def test_general_policy_requires_nonempty_allowlist():
    with pytest.raises(HpcMcpError) as caught:
        validate("pwd")
    assert caught.value.code == "COMMAND_BLOCKED"


def test_allowlist_matches_entire_command_and_blacklist_wins():
    with pytest.raises(HpcMcpError):
        validate("squeue --all", whitelist=["squeue"])
    with pytest.raises(HpcMcpError):
        validate("squeue --all", whitelist=["squeue.*"], blacklist=[".*--all"])
    result = validate("squeue --me", whitelist=["squeue.*"])
    assert result.render() == "cd -- /work && squeue --me"


@pytest.mark.parametrize("command", [
    "pwd; sbatch a", "pwd && sbatch a", "pwd || sbatch a", "pwd &",
    "pwd | cat", "pwd > out", "pwd < in", "pwd\ncat a", "pwd\rcat a",
    "echo `pwd`", "echo $(pwd)", "echo ${HOME}", "echo $HOME",
    "echo <(pwd)", "echo \x00", "echo \\x", "echo {a,b}",
])
def test_shell_controls_and_expansions_rejected(command):
    with pytest.raises(HpcMcpError) as caught:
        validate(command, whitelist=["(?s).*" ])
    assert caught.value.code == "COMMAND_BLOCKED"


@pytest.mark.parametrize("command", [
    "sbatch job", "/usr/bin/sbatch job", "srun app", "scancel 123",
    "bash -c sbatch", "sh script", "python3 script.py", "python3.12 script.py",
    "env sbatch job", "command sbatch job", "xargs sbatch", "nohup sbatch job",
    "scontrol update JobId=123", "salloc app", "eval sbatch", "exec sbatch",
    "sudo sbatch job", "perl script", "ruby script", "node script",
])
def test_submission_and_explicit_wrappers_are_blocked(command):
    with pytest.raises(HpcMcpError) as caught:
        validate(command, whitelist=[".*"])
    assert caught.value.code == "COMMAND_BLOCKED"


def test_non_builtin_program_can_be_explicitly_allowed_and_arguments_are_quoted():
    result = validate("/opt/tools/inspect 'a b'", whitelist=[r"/opt/tools/inspect 'a b'"])
    assert result.render() == "cd -- /work && /opt/tools/inspect 'a b'"


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), 600001])
def test_invalid_timeout_fails(timeout):
    with pytest.raises(HpcMcpError) as caught:
        policy.GeneralCommandPolicy(profile(whitelist=["pwd"])).validate("pwd", "/work", timeout)
    assert caught.value.code == "INVALID_TIMEOUT"


def test_timeout_is_minimum_of_call_and_profile():
    guard = policy.GeneralCommandPolicy(profile(whitelist=["pwd"], commandTimeoutMs=15000))
    assert guard.validate("pwd", "/work", 500).timeout_seconds == 0.5
    assert guard.validate("pwd", "/work", 30000).timeout_seconds == 15
    assert guard.validate("pwd", "/work", None).timeout_seconds == 15


@pytest.mark.parametrize("command", ["", "  ", "cat 'unterminated"])
def test_invalid_command_is_safe_error(command):
    with pytest.raises(HpcMcpError):
        validate(command, whitelist=[".*"])


@pytest.mark.parametrize("command", [
    "rg --pre=sbatch file", "tar --checkpoint-action=exec=sbatch file",
    "git -c alias.submit=sbatch submit",
    "rg --pre=/bin/sh file", "sed -e '1e sbatch job' file",
])
def test_explicit_wrapper_arguments_cannot_hide_submission(command):
    with pytest.raises(HpcMcpError) as error:
        validate(command, whitelist=[".*"])
    assert error.value.code == "COMMAND_BLOCKED"


def test_original_whitespace_is_not_normalized_before_allowlist_match():
    with pytest.raises(HpcMcpError):
        validate("cat  'a b'", whitelist=["cat 'a b'"])
    result = validate("cat  'a b'", whitelist=["cat  'a b'"])
    assert result.render() == "cd -- /work && cat 'a b'"


@pytest.mark.parametrize("command", [
    "git -c alias.x='!sbatch job.slurm' x",
    "git -calias.x='!sb''atch job.slurm' x",
    "git -c 'alias.x=!scheduler-submit job.slurm' x",
    "/usr/bin/git -c core.pager=scheduler-submit log",
    "git status",  # repository configuration can supply fsmonitor hooks
    "sed 'esbatch job.slurm' input",
    "sed -e 'e''sbatch job.slurm' input",
    "sed --expression='escheduler-submit job.slurm' input",
    "sed -f program.sed input",  # hidden script has the same capabilities
    "sed 's/a/scheduler-submit job.slurm/e' input",
    "awk 'system(\"scheduler-submit\")' input",
    "find . -exec scheduler-submit job.slurm +",
    "find . -execdir scheduler-submit job.slurm +",
    "perl -e 'system(\"scheduler-submit\")'",
    "ruby -e 'system(\"scheduler-submit\")'",
    "node -e 'require(\"child_process\").execSync(\"scheduler-submit\")'",
    "rg --pre=scheduler-submit data",
    "rg --pre scheduler-submit data",
    "sort --compress-program=scheduler-submit input",
    "sort --compress-program scheduler-submit input",
    "sort --comp=scheduler-submit input",
    "split --filter=scheduler-submit input",
    "split --fi scheduler-submit input",
    "tar --checkpoint=1 --checkpoint-action=exec=scheduler-submit -cf out input",
    "tar -I scheduler-submit -cf out input",
    "zip -T -TT scheduler-submit out.zip input",
    "make -f custom.mk",
    "cmake -P program.cmake",
    "less input",  # interactive shell escapes / configurable helper
    "vim -c '!scheduler-submit' input",
    "flock lock scheduler-submit job.slurm",
    "watch scheduler-submit job.slurm",
])
def test_secondary_execution_capabilities_are_blocked_even_with_allowlist(command):
    with pytest.raises(HpcMcpError) as error:
        validate(command, whitelist=[".*"])
    assert error.value.code == "COMMAND_BLOCKED"


@pytest.mark.parametrize("command, expected_args", [
    ("rg --fixed-strings 'a b' input", ("--no-config", "--fixed-strings", "a b", "input")),
    ("sort -n 'data file'", ("-n", "data file")),
    ("split -l 10 'data file'", ("-l", "10", "data file")),
    ("sort -- --compress-program=data", ("--", "--compress-program=data")),
    ("/opt/tools/inspect '!non-command'", ("!non-command",)),
])
def test_safe_utility_arguments_and_configured_custom_programs_remain_literal(command, expected_args):
    result = validate(command, whitelist=[".*"])
    assert result.args == expected_args


@pytest.mark.parametrize("command", [
    ". script", "trap 'scheduler-submit job.slurm' EXIT",
    "builtin helper", "enable -f library.so helper",
    "gsed 'escheduler-submit' input", "nawk 'system(\"scheduler-submit\")'",
    "gtar -I scheduler-submit -cf out input", "bsdtar --use-compress-program=scheduler-submit -cf out input",
    "gmake -f custom.mk", "ninja -f build.ninja",
    "vi input", "nvim input", "view input", "ex input", "ed input",
    "more input", "man topic",
    "time scheduler-submit", "stdbuf -oL scheduler-submit",
    "chrt 1 scheduler-submit", "ionice scheduler-submit", "taskset 1 scheduler-submit",
    "unshare scheduler-submit", "chroot directory scheduler-submit", "nsenter scheduler-submit",
    "screen scheduler-submit", "tmux new-session scheduler-submit",
    "tclsh script.tcl", "wish script.tcl", "julia script.jl", "Rscript script.R",
])
def test_known_execution_program_aliases_and_shell_builtins_are_blocked(command):
    with pytest.raises(HpcMcpError) as error:
        validate(command, whitelist=[".*"])
    assert error.value.code == "COMMAND_BLOCKED"


@pytest.mark.parametrize("command", [
    "split --additional-suffix -- --filter=helper input",
    "sp''lit --additional-suffix -- --filter=helper input",
    "rg -e -- --pre=helper data",
    "r''g -e -- --pre=helper data",
    "sort -o -- --compress-program=helper input",
    "so''rt -o -- --compress-program=helper input",
    "rg --regexp -- --pre helper data",
    "sort --output -- --comp helper input",
    "split --additional-suffix -- --fi helper input",
    "sort input --compress-program=helper",
    "rg -ie -- --pre=helper data",
    "split -a -- --filter=helper input",
])
def test_option_value_double_dash_does_not_hide_execution_options(command):
    with pytest.raises(HpcMcpError) as error:
        validate(command, whitelist=[".*"])
    assert error.value.code == "COMMAND_BLOCKED"


@pytest.mark.parametrize("command", [
    "rg --fixed-strings git data", "cat /work/git",
    "rg -e sbatch data", "sort -o /work/git input",
    "split --additional-suffix=git input", "inspect /usr/bin/scancel",
    "rg -e -- -- --pre=helper", "sort -o -- -- --compress-program=helper",
    "split --additional-suffix -- -- --filter=helper",
    "rg -ie-- -- --pre=helper", "sort -no-- -- --compress-program=helper",
])
def test_ordinary_data_arguments_are_not_interpreted_as_programs(command):
    assert validate(command, whitelist=[".*"]).program


@pytest.mark.parametrize("command", [
    "rg --unknown data", "sort --unknown input", "split --unknown input",
    "rg -Y data", "sort -Y input", "split -Y input",
    "sort --out input", "split --additional input", "rg --reg data",
    "rg -e", "sort --output", "split --additional-suffix",
    "rg --fixed-strings=true data", "sort --reverse=true input",
])
def test_unsupported_or_incomplete_utility_option_syntax_fails_closed(command):
    with pytest.raises(HpcMcpError) as error:
        validate(command, whitelist=[".*"])
    assert error.value.code == "COMMAND_BLOCKED"


def test_rg_execution_disables_inherited_configuration():
    command = validate("rg pattern data", whitelist=["rg pattern data"])
    assert command.args == ("--no-config", "pattern", "data")


@pytest.mark.parametrize("command", [
    "rg --hostname-bin=helper data", "rg --search-zip data", "rg -z data",
    "rg --no-config -e -- --hostname-bin=helper data",
])
def test_rg_other_external_helper_capabilities_are_rejected(command):
    with pytest.raises(HpcMcpError):
        validate(command, whitelist=[".*"])
