import json

import pytest
from pydantic import ValidationError

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.policy import CommandPolicy, RemoteCommand


@pytest.mark.parametrize(
    "program",
    [
        "sudo",
        "/usr/bin/su",
        "shutdown",
        "reboot",
        "systemctl",
        "service",
        "scancel",
        "rm",
        "rmdir",
        "mkfs",
        "dd",
        "chmod",
    ],
)
def test_policy_rejects_exact_first_release_denylist(program: str) -> None:
    command = RemoteCommand(program, ["target"], "/home/alice/project")

    with pytest.raises(HpcMcpError) as error:
        CommandPolicy().validate(command)

    assert error.value.code == "COMMAND_BLOCKED"
    assert error.value.details == {}


def test_policy_accepts_structured_python_command() -> None:
    command = RemoteCommand("python3", ["tests/smoke.py", "--quick"], "/home/alice/project")

    assert CommandPolicy().validate(command) == command


def test_policy_accepts_the_single_public_diagnostic_command() -> None:
    command = RemoteCommand.for_user("pwd", [], "/home/alice/project")

    assert CommandPolicy().validate(command) == command


@pytest.mark.parametrize(
    ("program", "args"),
    [
        ("bash", ["-c", "echo unsafe"]),
        ("/bin/bash", ["-c", "echo unsafe"]),
        ("sh", ["-c", "echo unsafe"]),
        ("/usr/bin/sh", ["-c", "echo unsafe"]),
        ("python3", ["-c", "print('unsafe')"]),
        ("/usr/bin/python3", ["-c", "print('unsafe')"]),
        ("python3", ["-m", "pip", "install", "unsafe"]),
        ("env", ["bash", "-c", "echo unsafe"]),
        ("command", ["bash", "-c", "echo unsafe"]),
        ("xargs", ["bash", "-c", "echo unsafe"]),
        ("pwd", ["unexpected"]),
    ],
)
def test_policy_rejects_public_command_bypasses(
    program: str, args: list[str]
) -> None:
    command = RemoteCommand.for_user(program, args, "/home/alice/project")

    with pytest.raises(HpcMcpError) as error:
        CommandPolicy().validate(command)

    assert error.value.code == "COMMAND_BLOCKED"


def test_policy_rejects_public_environment_for_diagnostic_command() -> None:
    command = RemoteCommand.for_user(
        "pwd", [], "/home/alice/project", {"PATH": "/tmp/unsafe"}
    )

    with pytest.raises(HpcMcpError) as error:
        CommandPolicy().validate(command)

    assert error.value.code == "COMMAND_BLOCKED"


@pytest.mark.parametrize(
    "environment",
    [
        {"lowercase": "value"},
        {"1INVALID": "value"},
        {"LD_PRELOAD": "/tmp/override.so"},
        {"PYTHONPATH": "/tmp/modules"},
        {"BASH_ENV": "/tmp/profile"},
        {"ENV": "/tmp/profile"},
    ],
)
def test_policy_rejects_invalid_or_dangerous_environment(environment: dict[str, str]) -> None:
    command = RemoteCommand("python3", ["job.py"], "/work", environment)

    with pytest.raises(HpcMcpError) as error:
        CommandPolicy().validate(command)

    assert error.value.code == "COMMAND_BLOCKED"
    assert error.value.details == {}


def test_render_quotes_cwd_arguments_and_sorted_environment_values() -> None:
    command = RemoteCommand(
        "python3",
        ["a script.py", "$(touch /tmp/never)"],
        "/home/alice/my project",
        {"Z_MODE": "two words", "A_MODE": "$(not-run)"},
    )

    rendered = CommandPolicy().validate(command).render()

    assert rendered == (
        "cd -- '/home/alice/my project' && "
        "A_MODE='$(not-run)' Z_MODE='two words' "
        "python3 'a script.py' '$(touch /tmp/never)'"
    )


def test_remote_command_rejects_unstructured_program_text() -> None:
    with pytest.raises(ValidationError):
        RemoteCommand("python3; reboot", [], "/work")


def test_remote_command_is_immutable() -> None:
    command = RemoteCommand("python3", ["job.py"], "/work")

    with pytest.raises(ValidationError):
        command.program = "reboot"


def test_remote_command_nested_collections_are_immutable() -> None:
    command = RemoteCommand(
        "python3",
        ["job.py"],
        "/work",
        {"SAFE_VALUE": "original"},
    )

    with pytest.raises(AttributeError):
        command.args.append("unexpected.py")

    with pytest.raises(TypeError):
        command.environment["UNSAFE_VALUE"] = "injected"

    assert command.render() == "cd -- /work && SAFE_VALUE=original python3 job.py"


def test_remote_command_serializes_immutable_environment() -> None:
    command = RemoteCommand("python3", ["job.py"], "/work", {"SAFE_VALUE": "value"})

    dumped = command.model_dump()

    assert type(dumped["environment"]) is dict
    assert json.loads(command.model_dump_json())["environment"] == {
        "SAFE_VALUE": "value"
    }
