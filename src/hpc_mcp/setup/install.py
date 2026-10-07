"""Explicit, staged local installation. Read-only planning is the default."""
from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import os
import shutil
import tempfile
import time
import zipfile
from email.parser import BytesParser
from pathlib import Path

from hpc_mcp.agent.app_server import generate_schema
from hpc_mcp.codex_executable import resolve_codex_executable
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.config.paths import AppPaths, resolve_servers_path
from hpc_mcp.container import build_container
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.setup.windows import InstanceLock, WindowsStartup, run_command


_REPLACE_RETRY_DELAYS = (0.01, 0.02, 0.04, 0.08, 0.16)


def _failed(code, message):
    return HpcMcpError(code, message)


def _checked(runner, args):
    result = runner(args)
    if result.returncode:
        raise _failed("SETUP_COMMAND_FAILED", "A required setup command failed; registration was not completed")
    return result


def _atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        # Retry only publication of the same closed, fsynced file. Windows
        # sharing/access conflicts get at most 0.31 seconds of total backoff.
        original_error = None
        for attempt in range(len(_REPLACE_RETRY_DELAYS) + 1):
            try:
                os.replace(temporary, path)
                break
            except OSError as error:
                winerror = getattr(error, "winerror", None)
                # CRT-only errors may expose EACCES without a native WinError.
                if os.name != "nt" or not (winerror in {5, 32}
                        or (winerror is None and error.errno == errno.EACCES)):
                    raise
                if original_error is None:
                    original_error = error
                if attempt == len(_REPLACE_RETRY_DELAYS):
                    raise original_error
                time.sleep(_REPLACE_RETRY_DELAYS[attempt])
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class CodexRegistration:
    """CLI adapter; keep credentials exclusively in servers.json.

    Existing registrations with env/cwd/HTTP/custom options are deliberately
    rejected before mutation because CLI re-add cannot faithfully restore them.
    """
    def __init__(self, *, runner=run_command, command=None):
        self.runner = runner
        self.command = command if command is not None else [resolve_codex_executable()]

    def get(self):
        result = self.runner([*self.command, "mcp", "get", "hpc-mcp", "--json"])
        try:
            if result.returncode:
                listed = _checked(self.runner, [*self.command, "mcp", "list", "--json"])
                entries = json.loads(listed.stdout)
                if not isinstance(entries, list) or any(item.get("name") == "hpc-mcp" for item in entries):
                    raise ValueError()
                return None
            entry = json.loads(result.stdout)
            transport = entry["transport"]
            if (transport["type"] != "stdio" or transport.get("env") or transport.get("env_vars")
                    or transport.get("cwd") or entry.get("enabled") is False
                    or any(entry.get(key) is not None for key in ("startup_timeout_sec", "tool_timeout_sec", "enabled_tools", "disabled_tools"))):
                raise ValueError()
            command = [transport["command"], *(transport.get("args") or [])]
            if not all(isinstance(arg, str) and arg for arg in command):
                raise ValueError()
            return command
        except (ValueError, TypeError, KeyError):
            raise _failed("LEGACY_REGISTRATION_UNSAFE", "Existing hpc-mcp registration is not recognized as installer-managed") from None

    def set(self, command):
        _checked(self.runner, [*self.command, "mcp", "add", "hpc-mcp", "--", *command])

    def remove(self):
        _checked(self.runner, [*self.command, "mcp", "remove", "hpc-mcp"])


class Installer:
    def __init__(self, config: Path, state: Path | None = None, skills: Path | None = None,
                 *, runner=run_command, registration=None, startup=None):
        self.config = resolve_servers_path(config)
        self.state = state or AppPaths.for_user().data_directory
        # Keep Windows binaries outside MSIX-expanded AppData paths. Explicit
        # state overrides retain their self-contained release layout.
        self.release_root = (Path.home() / ".hpc-codex-mcp" / "releases"
                             if os.name == "nt" and state is None else self.state / "releases")
        self.skills = skills or Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills"
        self.runner = runner
        self.registration = registration or CodexRegistration(runner=runner)
        self.startup = startup or WindowsStartup(runner=runner)
        self.metadata = self.state / "installation.json"

    def _profiles(self):
        profiles = ConfigRepository(self.config).load_all()
        for profile in profiles.values():
            if profile.host == "COMPUTE_HOST" or profile.username == "SSH_USERNAME":
                raise _failed("CONFIG_TEMPLATE", "Edit the SSH configuration template before installation")
        return profiles

    async def doctor(self, *, check_ssh=False):
        """Schema generation is local/model-free. SSH probes require opt-in."""
        profiles = self._profiles()
        await generate_schema(route="queue")
        results = []
        if check_ssh:
            container = build_container(config_file=self.config)
            for alias in profiles:
                result = await container.connection.test(alias)
                results.append({"server_alias": alias, "ready": result["ready"],
                                "capabilities": result["capabilities"]})
        return {"ready": all(item["ready"] for item in results), "config": "valid",
                "profile_count": len(profiles), "app_server": "compatible",
                "ssh": "checked" if check_ssh else "not_checked", "servers": results}

    def preflight(self):
        result = asyncio.run(self.doctor(check_ssh=True))
        if not result["ready"]:
            raise _failed("SLURM_UNAVAILABLE", "Required Slurm commands are unavailable")
        return result

    def prepare_release(self, *, wheel=None, source=None):
        """Install a wheel into an immutable release directory and smoke it.

        Existing environments/wheels are never replaced. A failed candidate is
        retained for diagnosis and cannot reach registration before a new smoke.
        """
        if (wheel is None) == (source is None):
            raise _failed("RELEASE_REQUIRED", "Supply exactly one --wheel or --source for installation")
        with tempfile.TemporaryDirectory(prefix="hpc-build-") as directory:
            if source:
                _checked(self.runner, ["uv", "build", "--wheel", "--out-dir", directory, str(Path(source).resolve())])
                candidates = list(Path(directory).glob("hpc_codex_mcp-*.whl"))
                if len(candidates) != 1:
                    raise _failed("BUILD_FAILED", "Build did not produce exactly one hpc-mcp wheel")
                wheel = candidates[0]
            wheel = Path(wheel).resolve()
            if not wheel.is_file() or not wheel.name.startswith("hpc_codex_mcp-") or wheel.suffix != ".whl":
                raise _failed("WHEEL_INVALID", "Expected an hpc-codex-mcp wheel")
            digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
            release = self.release_root / digest
            release.mkdir(parents=True, exist_ok=True)
            archived = release / wheel.name
            if not archived.exists():
                shutil.copyfile(wheel, archived)
            elif hashlib.sha256(archived.read_bytes()).hexdigest() != digest:
                raise _failed("WHEEL_CHANGED", "Archived wheel integrity check failed")
            environment = release / "venv"
            if not environment.exists():
                _checked(self.runner, ["uv", "venv", "--python", "3.12", str(environment)])
            python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            _checked(self.runner, ["uv", "pip", "install", "--link-mode", "copy", "--python", str(python), str(archived)])
            script = ("import importlib.resources as r; from hpc_mcp.server import create_server; "
                      "from hpc_mcp.agent.cli import app; from hpc_mcp.setup.cli import app; "
                      "print(r.files('hpc_mcp').joinpath('skills/hpc-slurm-workflow/SKILL.md'))")
            smoke = _checked(self.runner, [str(python), "-I", "-c", script])
            for entrypoint in ("hpc-mcp", "hpc-agent", "hpc-setup"):
                executable = python.parent / (entrypoint + ".exe" if os.name == "nt" else entrypoint)
                _checked(self.runner, [str(executable), "--help"])
            asset = Path(smoke.stdout.strip())
            _atomic_json(release / "release.json", {"wheel": wheel.name, "sha256": digest, "smoke": "passed"})
            return release, asset

    @property
    def pending(self):
        return self.state / "pending-installation.json"

    @property
    def destination(self):
        return self.skills / "hpc-slurm-workflow" / "SKILL.md"

    @staticmethod
    def _commands(release, config):
        python = Path(release) / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        pythonw = python.with_name("pythonw.exe") if os.name == "nt" else python
        return ([str(python), "-m", "hpc_mcp.server", "--config-file", str(config)],
                [str(pythonw), "-m", "hpc_mcp.agent.cli", "--config-file", str(config), "run"])

    @staticmethod
    def _fingerprint(record):
        return hashlib.sha256(json.dumps({k: v for k, v in record.items() if k != "fingerprint"},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _validate_record(self, record):
        """Accept only our versioned canonical descriptor, never legacy argv.

        The fingerprint detects damaged/stale metadata; it is not intended to
        authenticate against an attacker who can rewrite the user's files.
        """
        keys = {"format", "release", "config", "managed_command", "agent_command", "previous",
                "skill_source", "skill_sha256", "rollback_wheel", "fingerprint"}
        try:
            if not isinstance(record, dict):
                raise ValueError()
            # Check the entire bounded chain before serialization. This also
            # rejects cycles in in-memory descriptors without recursive calls.
            chain, seen, node = [], set(), record
            while node is not None:
                if (not isinstance(node, dict) or set(node) != keys or id(node) in seen
                        or len(chain) >= 16 or type(node["format"]) is not int or node["format"] not in {2, 3}):
                    raise ValueError()
                seen.add(id(node))
                chain.append(node)
                for key in ("release", "config", "skill_source"):
                    if not isinstance(node[key], str) or not Path(node[key]).is_absolute():
                        raise ValueError()
                command, agent = self._commands(node["release"], node["config"])
                if node["managed_command"] != command or node["agent_command"] != agent:
                    raise ValueError()
                for key in ("skill_sha256", "fingerprint"):
                    digest = node[key]
                    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                        raise ValueError()
                rollback = node["rollback_wheel"]
                if rollback is not None:
                    if not isinstance(rollback, dict) or set(rollback) != {"path", "sha256"}:
                        raise ValueError()
                    digest = rollback["sha256"]
                    if (not isinstance(digest, str) or len(digest) != 64
                            or any(c not in "0123456789abcdef" for c in digest)
                            or not isinstance(rollback["path"], str)
                            or Path(rollback["path"]).parent != self.state / "rollback" / digest):
                        raise ValueError()
                previous = node["previous"]
                if previous is None:
                    break
                if not isinstance(previous, dict):
                    raise ValueError()
                if node["format"] == 2:
                    if (set(previous) != {"release", "config"}
                            or not all(isinstance(p, str) and Path(p).is_absolute() for p in previous.values())):
                        raise ValueError()
                    break
                if (set(previous) != {"record", "skill_before", "startup_before"}
                        or type(previous["skill_before"]) is not bool
                        or type(previous["startup_before"]) is not bool
                        or not isinstance(previous["record"], dict)):
                    raise ValueError()
                node = previous["record"]
            for node in reversed(chain):
                if node["fingerprint"] != self._fingerprint(node):
                    raise ValueError()
            return record
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise _failed("LEGACY_REGISTRATION_UNSAFE", "Installation metadata does not identify a canonical managed registration") from None

    def _load(self):
        if not self.metadata.exists():
            return None
        try:
            return self._validate_record(json.loads(self.metadata.read_text(encoding="utf-8")))
        except (ValueError, OSError, RecursionError):
            raise _failed("LEGACY_REGISTRATION_UNSAFE", "Installation metadata cannot identify a managed registration") from None

    def _skill_hash(self):
        if self.destination.is_symlink():
            raise _failed("SKILL_CONFLICT", "The managed skill was replaced by a link")
        return hashlib.sha256(self.destination.read_bytes()).hexdigest() if self.destination.exists() else None

    def _check_skill(self, allowed):
        if self._skill_hash() not in allowed:
            raise _failed("SKILL_CONFLICT", "An unmanaged or edited skill was preserved")

    def _asset(self, record):
        data = Path(record["skill_source"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != record["skill_sha256"]:
            raise _failed("SKILL_ASSET_CHANGED", "The staged Skill failed its integrity check")
        return data

    def _temporary(self, journal):
        # Derive the only removable temporary path; never trust a journal path.
        return self.destination.with_name(".hpc-skill-" + journal["target"]["fingerprint"] + ".tmp")

    def _phase(self, journal, phase):
        journal["phase"] = phase
        _atomic_json(self.pending, journal)

    def _publish(self, record, journal):
        data = self._asset(record)
        if journal["phase"] != "compensating":
            self._phase(journal, "skill-publishing")
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary(journal)
        temporary.unlink(missing_ok=True)
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # External setup actions and staging may have raced with a user edit.
        self._check_skill(self._transaction_skill_hashes(journal))
        os.replace(temporary, self.destination)

    @staticmethod
    def _prior_command(record):
        previous = record["previous"]
        if not previous:
            return None
        if record["format"] == 3:
            return previous["record"]["managed_command"]
        # Older journals may already have restored this canonical command.
        # Permit compensation, but never use this partial state for a new undo.
        return Installer._commands(previous["release"], previous["config"])[0]

    def _check_registration(self, allowed):
        current = self.registration.get()
        if current not in allowed:
            raise _failed("INSTALLATION_CHANGED", "Managed registration changed during setup; it was preserved")
        return current

    def _set_registration(self, command, *, allowed=None):
        current = self._check_registration(allowed) if allowed is not None else self.registration.get()
        if current != command:
            self.registration.set(command) if command else self.registration.remove()

    def _transaction_commands(self, journal):
        record, prior = journal["target"], journal["prior"]
        old_command = (prior["managed_command"] if prior else None) if journal["operation"] == "install" else self._prior_command(record)
        return record["managed_command"], old_command

    @staticmethod
    def _transaction_skill_hashes(journal):
        record, prior = journal["target"], journal["prior"]
        return {None, record["skill_sha256"], prior["skill_sha256"] if prior else None}

    @staticmethod
    def _transaction_startup_commands(journal):
        record, prior = journal["target"], journal["prior"]
        return [None, record["agent_command"], prior["agent_command"] if prior else None]

    def _check_transaction(self, journal):
        self._check_registration(self._transaction_commands(journal))
        self.startup.verify(self._transaction_startup_commands(journal))
        self._check_skill(self._transaction_skill_hashes(journal))

    def _complete_install(self, journal):
        record = journal["target"]
        self._check_transaction(journal)
        self._asset(record)
        # Every phase after prepared directs recovery to compensation. Persist
        # it before the action: an interrupted/failed write must not start it.
        self._phase(journal, "registering")
        self._set_registration(record["managed_command"], allowed=self._transaction_commands(journal))
        self._phase(journal, "startup-installing")
        self.startup.verify(self._transaction_startup_commands(journal))
        self.startup.install(record["agent_command"], allowed=self._transaction_startup_commands(journal))
        self._publish(record, journal)
        self._phase(journal, "skill-published")
        _atomic_json(self.metadata, record)
        self.pending.unlink()

    def _compensate(self, journal):
        """Normalize all recovery failures, including durable journal writes."""
        try:
            self._restore_prior(journal)
        except Exception as error:
            if isinstance(error, HpcMcpError) and error.code == "INSTALLATION_RECOVERY_FAILED":
                raise
            raise _failed("INSTALLATION_RECOVERY_FAILED",
                "Setup compensation is pending; retry setup after resolving the external failure") from None

    def _restore_prior(self, journal):
        """Reverse effects, attempting every compensation even if one fails."""
        # Existing action phases already require rollback. This records that
        # rollback has begun; its failure leaves the same recovery direction.
        self._phase(journal, "compensating")
        record = journal["target"] if journal["operation"] == "uninstall" else journal["prior"]
        expected_command = record["managed_command"] if record else None
        expected_startup = record["agent_command"] if record and journal["startup_before"] else None
        expected_skill = record["skill_sha256"] if record and journal["skill_before"] else None
        def skill():
            if record and journal["skill_before"]:
                self._publish(record, journal)
            else:
                self._check_skill(self._transaction_skill_hashes(journal))
                self.destination.unlink(missing_ok=True)
            self._temporary(journal).unlink(missing_ok=True)
        def startup():
            self.startup.verify(self._transaction_startup_commands(journal))
            if expected_startup:
                self.startup.install(expected_startup, allowed=self._transaction_startup_commands(journal))
            else:
                self.startup.uninstall(allowed=self._transaction_startup_commands(journal))
        def registration():
            self._set_registration(expected_command, allowed=self._transaction_commands(journal))
        failures = []
        # Ownership is local to each action: one unavailable resource cannot
        # prevent restoration of another. Verify all effects before committing.
        for action in (skill, startup, registration,
                       lambda: self._check_skill({expected_skill}),
                       lambda: self.startup.verify([expected_startup]),
                       lambda: self._check_registration([expected_command])):
            try:
                action()
            except Exception as error:
                failures.append(error)
        if failures:
            conflict = any(isinstance(error, HpcMcpError) and error.code == "SKILL_CONFLICT" for error in failures)
            message = ("SKILL_CONFLICT: an unmanaged or edited Skill was preserved; " if conflict else "")
            raise _failed("INSTALLATION_RECOVERY_FAILED", message +
                "Setup compensation is pending; retry setup after resolving the external failure") from None
        if record:
            _atomic_json(self.metadata, record)
        else:
            self.metadata.unlink(missing_ok=True)
        self.pending.unlink()

    def _recover_pending(self):
        if not self.pending.exists():
            return
        try:
            journal = json.loads(self.pending.read_text(encoding="utf-8"))
            if set(journal) != {"operation", "phase", "target", "prior", "skill_before", "startup_before"}:
                raise ValueError()
            if journal["operation"] not in {"install", "uninstall"}:
                raise ValueError()
            self._validate_record(journal["target"])
            if journal["prior"] is not None:
                self._validate_record(journal["prior"])
            if journal["operation"] == "uninstall" and journal["target"]["format"] == 3:
                previous = journal["target"]["previous"]
                if journal["prior"] != (previous["record"] if previous else None):
                    raise ValueError()
            if type(journal["skill_before"]) is not bool or type(journal["startup_before"]) is not bool:
                raise ValueError()
        except (OSError, ValueError, KeyError, TypeError, RecursionError):
            raise _failed("INSTALLATION_INTERRUPTED", "Setup journal is invalid; existing integrations were preserved") from None
        # Only prepared guarantees that no forward external action started.
        # Treat legacy forward phases conservatively too, without a schema change.
        if journal["operation"] == "uninstall" or journal["phase"] != "prepared":
            self._compensate(journal)
        else:
            try:
                self._complete_install(journal)
            except Exception:
                self._compensate(journal)
                raise _failed("INSTALLATION_FAILED", "Installation failed and the prior managed state was restored") from None

    def _backup_wheel(self, wheel):
        original = Path(wheel).resolve()
        try:
            if not original.name.startswith("hpc_codex_mcp-") or original.suffix != ".whl":
                raise ValueError()
            with zipfile.ZipFile(original) as archive:
                entries = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
                if len(entries) != 1:
                    raise ValueError()
                metadata = BytesParser().parsebytes(archive.read(entries[0]))
                if metadata.get("Name", "").lower().replace("_", "-") != "hpc-codex-mcp" or not metadata.get("Version"):
                    raise ValueError()
        except (OSError, ValueError, zipfile.BadZipFile):
            raise _failed("WHEEL_INVALID", "Expected a valid hpc-codex-mcp rollback wheel") from None
        digest = hashlib.sha256(original.read_bytes()).hexdigest()
        backup = self.state / "rollback" / digest / original.name
        backup.parent.mkdir(parents=True, exist_ok=True)
        if not backup.exists():
            shutil.copyfile(original, backup)
        if hashlib.sha256(backup.read_bytes()).hexdigest() != digest:
            raise _failed("WHEEL_CHANGED", "Rollback wheel integrity check failed")
        return {"path": str(backup), "sha256": digest}

    def install(self, *, apply=False, wheel=None, source=None, rollback_wheel=None):
        self._profiles()
        if not apply:
            return {"apply": False, "actions": ["check SSH/Slurm and Codex schema", "isolate wheel and smoke test",
                "register hpc-mcp", "register current-user startup", "install hpc-slurm-workflow"],
                "requires": "--apply and exactly one --wheel or --source"}
        with InstanceLock(self.state / "setup.lock"):
            self._recover_pending()
            previous = self.registration.get()
            old = self._load()
            if previous is not None and (not old or previous != old["managed_command"]):
                raise _failed("LEGACY_REGISTRATION_UNSAFE", "Existing hpc-mcp registration is not recognized as installer-managed")
            if old and previous is None:
                raise _failed("INSTALLATION_CHANGED", "Managed MCP registration was removed outside setup")
            startup_before = self.startup.verify([old["agent_command"] if old else None, None])
            self._check_skill({None, old["skill_sha256"] if old else None})
            self.preflight()
            release, asset = self.prepare_release(wheel=wheel, source=source)
            if not asset.is_file():
                raise _failed("SKILL_ASSET_MISSING", "The wheel does not contain hpc-slurm-workflow; build the Task 7 asset first")
            command, agent = self._commands(release, self.config)
            rollback = old.get("rollback_wheel") if old else None
            if rollback_wheel:
                rollback = self._backup_wheel(rollback_wheel)
            same_release = old and previous == command
            record = {"format": old["format"] if same_release else 3, "managed_command": command, "agent_command": agent,
                "rollback_wheel": rollback, "config": str(self.config),
                "previous": (old["previous"] if same_release else
                    {"record": old, "skill_before": self.destination.exists(), "startup_before": startup_before} if old else None),
                "release": str(release), "skill_source": str(asset),
                "skill_sha256": hashlib.sha256(asset.read_bytes()).hexdigest()}
            record["fingerprint"] = self._fingerprint(record)
            self._validate_record(record)
            journal = {"operation": "install", "phase": "prepared", "target": record, "prior": old,
                       "skill_before": self.destination.exists(), "startup_before": startup_before}
            _atomic_json(self.pending, journal)
            try:
                self._complete_install(journal)
            except Exception:
                self._compensate(journal)
                raise _failed("INSTALLATION_FAILED", "Installation failed and the prior managed state was restored") from None
            return {"apply": True, "installed": True, "release": str(release)}

    def uninstall(self, *, apply=False):
        if not apply:
            return {"apply": False, "actions": ["restore previous managed installation or remove managed integrations"],
                    "preserved": ["configuration", "databases", "watches", "release wheels"]}
        with InstanceLock(self.state / "setup.lock"):
            self._recover_pending()
            record = self._load()
            if not record:
                return {"apply": True, "installed": False}
            previous = record["previous"]
            if previous and record["format"] == 2:
                raise _failed("LEGACY_REGISTRATION_UNSAFE",
                    "Older upgrade metadata lacks the complete prior installation; existing integrations were preserved")
            prior = previous["record"] if previous else None
            if self.registration.get() != record["managed_command"]:
                raise _failed("INSTALLATION_CHANGED", "Managed MCP registration changed outside setup")
            self._check_skill({None, record["skill_sha256"]})
            startup_before = self.startup.verify([None, record["agent_command"]])
            # Validate a restorable source before removing the installed Skill.
            if self.destination.exists():
                self._asset(record)
            if prior and previous["skill_before"]:
                self._asset(prior)
            journal = {"operation": "uninstall", "phase": "prepared", "target": record, "prior": prior,
                       "skill_before": self.destination.exists(), "startup_before": startup_before}
            _atomic_json(self.pending, journal)
            try:
                self._phase(journal, "registration-removing")
                self._set_registration(self._prior_command(record), allowed=self._transaction_commands(journal))
                self._phase(journal, "startup-removing")
                self.startup.verify(self._transaction_startup_commands(journal))
                if prior and previous["startup_before"]:
                    self.startup.install(prior["agent_command"], allowed=self._transaction_startup_commands(journal))
                else:
                    self.startup.uninstall(allowed=self._transaction_startup_commands(journal))
                self._phase(journal, "skill-removing")
                if prior and previous["skill_before"]:
                    self._publish(prior, journal)
                else:
                    self._check_skill(self._transaction_skill_hashes(journal))
                    self.destination.unlink(missing_ok=True)
                if prior:
                    _atomic_json(self.state / "last-uninstalled.json", record)
                    _atomic_json(self.metadata, prior)
                else:
                    self.metadata.replace(self.state / "last-uninstalled.json")
                self.pending.unlink()
            except Exception:
                self._compensate(journal)
                raise _failed("UNINSTALLATION_FAILED", "Uninstall failed and the managed installation was restored") from None
            return {"apply": True, "installed": False}
