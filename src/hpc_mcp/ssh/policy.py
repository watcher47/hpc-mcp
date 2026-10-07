import re
import shlex
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.config.models import ClusterConfig


class RemoteCommand(BaseModel):
    model_config = ConfigDict(frozen=True, validate_default=True)

    program: str = Field(pattern=r"^[A-Za-z0-9_./+-]+$")
    args: tuple[str, ...] = Field(default_factory=tuple)
    cwd: str
    environment: Mapping[str, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=30.0, gt=0, le=600)
    origin: Literal["service", "user"] = Field(
        default="service", exclude=True, repr=False
    )

    def __init__(
        self,
        program: str,
        args: Sequence[str] | None = None,
        cwd: str | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
        **data: Any,
    ) -> None:
        values: dict[str, Any] = {
            "program": program,
            "timeout_seconds": timeout_seconds,
            **data,
        }
        if args is not None:
            values["args"] = args
        if cwd is not None:
            values["cwd"] = cwd
        if environment is not None:
            values["environment"] = environment
        super().__init__(**values)

    @classmethod
    def for_user(
        cls,
        program: str,
        args: Sequence[str] | None = None,
        cwd: str | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
    ) -> "RemoteCommand":
        """Build a command from the public ``remote_execute`` MCP input only."""
        return cls(
            program,
            args,
            cwd,
            environment,
            timeout_seconds,
            origin="user",
        )

    @field_validator("environment")
    @classmethod
    def _freeze_environment(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return MappingProxyType(dict(value))

    @field_serializer("environment")
    def _serialize_environment(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    def render(self) -> str:
        environment = " ".join(
            f"{key}={shlex.quote(value)}" for key, value in sorted(self.environment.items())
        )
        invocation = shlex.join([self.program, *self.args])
        prefix = f"cd -- {shlex.quote(self.cwd)} && "
        return f"{prefix}{environment} {invocation}" if environment else f"{prefix}{invocation}"


class CommandPolicy:
    _service_programs = frozenset({"which", "pwd", "rg", "python3", "squeue", "sacct"})
    _environment_key = re.compile(r"^[A-Z_][A-Z0-9_]*$")
    _blocked_environment = frozenset({"LD_PRELOAD", "PYTHONPATH", "BASH_ENV", "ENV"})
    _interpreter_evaluation_flags = frozenset({"-c", "-m", "--command", "--module"})

    def validate(self, command: RemoteCommand) -> RemoteCommand:
        if any(
            self._environment_key.fullmatch(key) is None
            or key in self._blocked_environment
            for key in command.environment
        ):
            raise HpcMcpError("COMMAND_BLOCKED", "Remote command environment is not permitted")

        if command.origin == "user":
            if command.program != "pwd" or command.args or command.environment:
                raise HpcMcpError("COMMAND_BLOCKED", "Remote command is not permitted")
            return command

        if command.program not in self._service_programs:
            raise HpcMcpError("COMMAND_BLOCKED", "Remote command is not permitted")
        if PurePosixPath(command.program).name != command.program:
            raise HpcMcpError("COMMAND_BLOCKED", "Remote command is not permitted")
        if command.program == "python3" and any(
            argument in self._interpreter_evaluation_flags
            or argument.startswith("-c")
            or argument.startswith("-m")
            for argument in command.args
        ):
            raise HpcMcpError("COMMAND_BLOCKED", "Remote command is not permitted")
        return command


class GeneralCommandPolicy:
    """Config-owned full-command permission, separate from project execution.

    The allowlist is a trust boundary: owners must only allow programs whose
    implementation they trust. String validation cannot sandbox an executable.
    """

    _blocked_programs = frozenset({
        # Scheduler mutation belongs to the preview / submission services.
        "sbatch", "srun", "salloc", "scancel", "scontrol", "sacctmgr",
        # Shells and programs which dispatch another executable.
        "sh", "bash", "dash", "zsh", "fish", "csh", "tcsh", "ksh",
        "env", "command", "exec", "eval", "source", ".", "xargs", "nohup",
        "builtin", "enable", "trap",
        "sudo", "su", "doas", "timeout", "nice", "setsid", "busybox",
        "flock", "watch", "time", "stdbuf", "chrt", "ionice", "taskset",
        "unshare", "chroot", "nsenter", "screen", "tmux",
        # Script languages are execution capabilities, even when their source
        # resembles data processing (sed e, awk system, find -exec/-execdir).
        "perl", "ruby", "node", "nodejs", "php", "lua", "awk", "gawk",
        "mawk", "nawk", "sed", "gsed", "find", "parallel", "ssh", "sftp", "scp", "expect",
        "tclsh", "wish", "julia", "rscript",
        # Configuration, scripts, helpers or interactive escapes can execute
        # commands without naming them in argv. Fail closed for these programs;
        # do not attempt to recognize only e.g. Git's -c alias.* spelling.
        "git", "tar", "gtar", "bsdtar", "zip", "make", "gmake", "cmake", "ninja",
        "less", "more", "man", "vim", "vi", "nvim", "view", "ex", "ed",
    })
    # Exact supported option spellings and their arity. Unknown/abbreviated
    # options fail closed; an unrecognized option may consume the next '--'.
    # Options which execute helpers deliberately do not appear here.
    # Tuples: short flags, short required-value options, long flags,
    # long required-value options, long optional (=value only) options.
    _utility_options = {
        "rg": (
            "sFivxUPSawL.ubhnN0opqHIclV", "efEmjgdtTABCMr",
            "case-sensitive crlf fixed-strings ignore-case invert-match line-regexp "
            "mmap multiline multiline-dotall no-unicode null-data pcre2 smart-case "
            "stop-on-nonmatch text word-regexp auto-hybrid-regex no-pcre2-unicode "
            "binary follow glob-case-insensitive hidden ignore-file-case-insensitive "
            "no-ignore no-ignore-dot no-ignore-exclude no-ignore-files no-ignore-global "
            "no-ignore-parent no-ignore-vcs no-require-git one-file-system unrestricted "
            "block-buffered byte-offset column heading help include-zero line-buffered "
            "line-number no-line-number max-columns-preview null only-matching "
            "passthru pretty quiet trim vimgrep with-filename no-filename sort-files "
            "count count-matches files-with-matches files-without-match json debug "
            "no-ignore-messages no-messages stats trace files no-config pcre2-version "
            "type-list version no-pre no-heading no-context-separator",
            "regexp file dfa-size-limit encoding engine max-count regex-size-limit "
            "threads glob iglob ignore-file max-depth max-filesize type type-not "
            "type-add type-clear after-context before-context color colors context "
            "context-separator field-context-separator field-match-separator "
            "hyperlink-format max-columns path-separator replace sort sortr generate",
            "",
        ),
        "sort": (
            "bdfgiMhnRrVcCmsuz", "koStT",
            "ignore-leading-blanks dictionary-order ignore-case general-numeric-sort "
            "ignore-nonprinting month-sort human-numeric-sort numeric-sort random-sort "
            "reverse version-sort debug merge stable unique zero-terminated help version",
            "random-source sort batch-size files0-from key output buffer-size "
            "field-separator temporary-directory parallel",
            "check",
        ),
        "split": (
            "dxeu", "abClnt",
            "elide-empty-files unbuffered verbose help version",
            "suffix-length additional-suffix bytes line-bytes lines number separator",
            "numeric-suffixes hex-suffixes",
        ),
    }

    def __init__(self, config: ClusterConfig) -> None:
        self._config = config

    def validate(self, cmd_string: str, directory: str,
                 timeout_ms: int | None) -> RemoteCommand:
        blocked = HpcMcpError("COMMAND_BLOCKED", "Command is not permitted by the SSH policy")
        if not self._config.whitelist or not cmd_string.strip():
            raise blocked
        # Do not interpret expansions, shell operators, or escaped spellings.
        if any(char in cmd_string for char in ";&|<>`$\\{}\r\n\x00"):
            raise blocked
        if any(ord(char) < 32 and char != "\t" for char in cmd_string):
            raise blocked
        if not any(re.fullmatch(pattern, cmd_string) for pattern in self._config.whitelist):
            raise blocked
        if any(re.search(pattern, cmd_string) for pattern in self._config.blacklist):
            raise blocked
        try:
            argv = shlex.split(cmd_string, posix=True)
        except ValueError:
            raise blocked from None
        if not argv:
            raise blocked
        # PurePosixPath('.').name is empty, but '.' is the shell's source builtin.
        name = argv[0].rsplit("/", 1)[-1].lower()
        if self._forbidden_name(name) or self._has_unsupported_options(name, argv[1:]):
            raise blocked
        if timeout_ms is not None and (
            type(timeout_ms) is not int or not 0 < timeout_ms <= 600_000
        ):
            raise HpcMcpError("INVALID_TIMEOUT", "SSH timeout must be a positive bounded integer")
        timeout = min(timeout_ms or self._config.command_timeout_ms,
                      self._config.command_timeout_ms) / 1000
        # RIPGREP_CONFIG_PATH could inject --pre or another helper outside the
        # validated argv. --no-config disables that implicit source of options.
        args = ["--no-config", *argv[1:]] if name == "rg" else argv[1:]
        try:
            return RemoteCommand(argv[0], args, directory, timeout_seconds=timeout)
        except ValueError:
            raise blocked from None

    @classmethod
    def _has_unsupported_options(cls, name: str, args: Sequence[str]) -> bool:
        """Parse option arity before recognizing an end-of-options marker.

        Scan past operands (GNU permutation), consume attached/separate required
        values, and accept optional values only with '='. Genuine '--' ends the
        scan. Operand text is never interpreted as a program name. Custom
        executables outside this table remain the config owner's trust boundary.
        """
        specification = cls._utility_options.get(name)
        if specification is None:
            return False
        short_flags, short_values, flags, values, optional = specification
        long_flags, long_values, long_optional = (
            set(group.split()) for group in (flags, values, optional)
        )
        index = 0
        while index < len(args):
            arg = args[index]
            index += 1
            if arg == "--":
                return False
            if arg.startswith("--"):
                option, equals, _ = arg[2:].partition("=")
                if option in long_values:
                    if not equals:
                        if index == len(args):
                            return True
                        index += 1
                elif option in long_optional:
                    pass  # GNU optional values never consume the next argv.
                elif option not in long_flags or equals:
                    return True
            elif arg.startswith("-") and arg != "-":
                for position, option in enumerate(arg[1:], start=1):
                    if option in short_values:
                        if position == len(arg) - 1:
                            if index == len(args):
                                return True
                            index += 1
                        break  # Rest of the cluster, if any, is the value.
                    if option not in short_flags:
                        return True
        return False

    @classmethod
    def _forbidden_name(cls, name: str) -> bool:
        return name in cls._blocked_programs or re.fullmatch(
            r"(?:python|pypy|perl|ruby|lua|php|node|bash|sh)(?:\d+(?:\.\d+)*)?", name
        ) is not None
