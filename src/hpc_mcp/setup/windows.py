"""Current-user startup and process locking; no shell or credential arguments."""
from __future__ import annotations

import csv
import hashlib
import io
import locale
import os
import subprocess
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET

from hpc_mcp.errors import HpcMcpError

MARKER = "hpc-codex-mcp managed agent startup v1"
NS = "http://schemas.microsoft.com/windows/2004/02/mit/task"


def account_sid(account: str) -> str | None:
    """Resolve one Windows account using Unicode APIs; never parse console text."""
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    lookup = advapi.LookupAccountNameW
    lookup.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, ctypes.c_void_p,
                       ctypes.POINTER(wintypes.DWORD), wintypes.LPWSTR,
                       ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(wintypes.DWORD)]
    lookup.restype = wintypes.BOOL
    size, domain_size, use = wintypes.DWORD(), wintypes.DWORD(), wintypes.DWORD()
    lookup(None, account, None, ctypes.byref(size), None, ctypes.byref(domain_size), ctypes.byref(use))
    if ctypes.get_last_error() != 122 or not size.value:
        return None
    sid = ctypes.create_string_buffer(size.value)
    domain = ctypes.create_unicode_buffer(max(domain_size.value, 1))
    if not lookup(None, account, sid, ctypes.byref(size), domain, ctypes.byref(domain_size), ctypes.byref(use)):
        return None
    # SID_NAME_USE SidTypeUser only: groups/aliases cannot prove user ownership.
    if use.value != 1:
        return None
    convert = advapi.ConvertSidToStringSidW
    convert.argtypes = [ctypes.c_void_p, ctypes.POINTER(wintypes.LPWSTR)]
    convert.restype = wintypes.BOOL
    result = wintypes.LPWSTR()
    if not convert(sid, ctypes.byref(result)):
        return None
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    try:
        return result.value
    finally:
        kernel.LocalFree(ctypes.cast(result, ctypes.c_void_p))


def run_command(args, **kwargs):
    try:
        result = subprocess.run(args, capture_output=True, timeout=120,
            **({"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}), **kwargs)
        def decode(value):
            if value.startswith((b"\xff\xfe", b"\xfe\xff")):
                return value.decode("utf-16", errors="replace")
            try:
                return value.decode("utf-8-sig")
            except UnicodeError:
                # Native utilities retain the system code page in Python UTF-8 mode.
                return value.decode(locale.getencoding(), errors="replace")
        return subprocess.CompletedProcess(args, result.returncode, decode(result.stdout), decode(result.stderr))
    except (OSError, subprocess.SubprocessError):
        raise HpcMcpError("SETUP_COMMAND_FAILED", "A required setup command failed") from None


class InstanceLock:
    """OS-owned advisory lock: survives stale files, releases on process death."""
    def __init__(self, path: Path):
        self.path = path
        self.stream = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        if os.fstat(self.stream.fileno()).st_size == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            self.stream = None
            raise HpcMcpError("AGENT_ALREADY_RUNNING", "The agent is already running") from None
        return self

    def __exit__(self, *_):
        if self.stream:
            # Closing the handle releases the OS lock; never unlink the file,
            # which could let another process lock a different inode.
            self.stream.close()
            self.stream = None


class WindowsStartup:
    def __init__(self, sid: str | None = None, *, runner=run_command, platform=None,
                 account_sid_lookup=account_sid):
        self.runner = runner
        self.platform = platform or os.name
        self.sid = sid
        self.account_sid_lookup = account_sid_lookup

    def _identity(self):
        if self.platform != "nt":
            raise HpcMcpError("PLATFORM_UNSUPPORTED", "Startup installation requires Windows")
        if not self.sid:
            result = self.runner(["whoami", "/user", "/fo", "csv", "/nh"])
            try:
                self.sid = next(csv.reader(io.StringIO(result.stdout)))[1]
                if result.returncode or not self.sid.startswith("S-1-"):
                    raise ValueError()
            except (IndexError, StopIteration, ValueError):
                raise HpcMcpError("STARTUP_IDENTITY_FAILED", "Could not identify the current Windows user") from None
        return self.sid

    @property
    def name(self):
        return "hpc-codex-agent-" + hashlib.sha256(self._identity().encode()).hexdigest()[:16]

    def _existing(self):
        result = self.runner(["schtasks", "/Query", "/TN", self.name, "/XML"])
        if result.returncode:
            # Do not interpret a localized error as absence. Confirm that the
            # scheduler can list tasks and the exact task is absent.
            listed = self.runner(["schtasks", "/Query", "/FO", "CSV", "/NH"])
            if listed.returncode or any(row and row[0].lstrip("\\") == self.name
                    for row in csv.reader(io.StringIO(listed.stdout))):
                raise HpcMcpError("STARTUP_QUERY_FAILED", "Could not inspect managed startup task")
            return None
        if not result.stdout.strip():
            return None
        try:
            root = ET.fromstring(result.stdout)
            if root.tag != f"{{{NS}}}Task":
                raise ValueError()

            # Extract only direct canonical fields below. Separately reject
            # security fields hidden in otherwise harmless scheduler metadata
            # or settings; they must never act as alternate ownership evidence.
            locations = {
                "Task": {"Task"},
                "RegistrationInfo": {"Task/RegistrationInfo"},
                "Description": {"Task/RegistrationInfo/Description"},
                "Triggers": {"Task/Triggers"},
                "LogonTrigger": {"Task/Triggers/LogonTrigger"},
                "Principals": {"Task/Principals"},
                "Principal": {"Task/Principals/Principal"},
                "UserId": {"Task/Triggers/LogonTrigger/UserId", "Task/Principals/Principal/UserId"},
                "LogonType": {"Task/Principals/Principal/LogonType"},
                "RunLevel": {"Task/Principals/Principal/RunLevel"},
                "Actions": {"Task/Actions"},
                "Exec": {"Task/Actions/Exec"},
                "Command": {"Task/Actions/Exec/Command"},
                "Arguments": {"Task/Actions/Exec/Arguments"},
            }
            pending = [(root, "Task")]
            while pending:
                parent, path = pending.pop()
                seen = set()
                for node in parent:
                    tag = node.tag.rsplit("}", 1)[-1]
                    node_path = f"{path}/{tag}"
                    if tag in locations:
                        if (node.tag != f"{{{NS}}}{tag}" or node_path not in locations[tag]
                                or (tag in seen and tag != "Principal")):
                            raise ValueError()
                        seen.add(tag)
                    pending.append((node, node_path))

            def one(parent, tag):
                nodes = parent.findall(f"{{{NS}}}{tag}")
                if len(nodes) != 1:
                    raise ValueError()
                return nodes[0]

            def text(parent, tag):
                node = one(parent, tag)
                if len(node):
                    raise ValueError()
                return node.text or ""

            def require_text(parent, tag, expected):
                if text(parent, tag) != expected:
                    raise ValueError()

            def require_identity(parent):
                identity = text(parent, "UserId")
                if identity == self.sid:
                    return
                # Require account form; malformed/foreign SID strings and an
                # empty any-user trigger must never become a lookup fallback.
                if (not identity or identity.strip() != identity or "\\" not in identity
                        or identity.startswith("S-")
                        or self.account_sid_lookup(identity) != self.sid):
                    raise ValueError()

            actions = one(root, "Actions")
            context = actions.get("Context")
            if not context or not context.strip():
                raise ValueError()
            # Actions selects the execution account; the logon trigger alone
            # does not prove which account runs the task. Resolve direct nodes
            # uniquely so duplicate or nested identity fields cannot hide it.
            principals = [node for node in one(root, "Principals").findall(f"{{{NS}}}Principal")
                          if node.get("id") == context]
            if len(principals) != 1:
                raise ValueError()
            principal = principals[0]
            require_identity(principal)
            require_text(principal, "LogonType", "InteractiveToken")
            # Task Scheduler omits the schema default LeastPrivilege.
            if principal.findall(f"{{{NS}}}RunLevel"):
                require_text(principal, "RunLevel", "LeastPrivilege")
            triggers = one(root, "Triggers")
            if len(triggers) != 1:
                raise ValueError()
            require_identity(one(triggers, "LogonTrigger"))
            require_text(one(root, "RegistrationInfo"), "Description", MARKER)
            action = one(actions, "Exec")
            if len(actions) != 1 or len(action) != 2:
                raise ValueError()
            command = text(action, "Command")
            arguments = text(action, "Arguments")
            if not command.strip():
                raise ValueError()
        except (ET.ParseError, ValueError):
            raise HpcMcpError("STARTUP_CONFLICT", "An unrelated startup entry occupies the managed name") from None
        # Consumers receive only validated values, never the untrusted XML.
        # Empty <Arguments/> is canonical for a command with no arguments.
        return command, arguments

    def install(self, command: list[str], *, allowed=None):
        """Create startup; transactions must supply their permitted prior commands."""
        sid = self._identity()
        root = ET.Element("Task", version="1.2", xmlns=NS)
        def add(parent, tag, value=None):
            node = ET.SubElement(parent, tag)
            node.text = value
            return node
        add(add(root, "RegistrationInfo"), "Description", MARKER)
        trigger = add(add(root, "Triggers"), "LogonTrigger")
        add(trigger, "Enabled", "true")
        add(trigger, "UserId", sid)
        principal = add(add(root, "Principals"), "Principal")
        principal.set("id", "Author")
        add(principal, "UserId", sid)
        add(principal, "LogonType", "InteractiveToken")
        add(principal, "RunLevel", "LeastPrivilege")
        settings = add(root, "Settings")
        for tag, value in {"MultipleInstancesPolicy": "IgnoreNew", "DisallowStartIfOnBatteries": "false",
                "StopIfGoingOnBatteries": "false", "ExecutionTimeLimit": "PT0S", "StartWhenAvailable": "true"}.items():
            add(settings, tag, value)
        actions = add(root, "Actions")
        actions.set("Context", "Author")
        action = add(actions, "Exec")
        add(action, "Command", command[0])
        add(action, "Arguments", subprocess.list2cmdline(command[1:]))
        existing = self._existing()
        if allowed is not None:
            self._verify_existing(existing, allowed)
        if existing == (command[0], subprocess.list2cmdline(command[1:])):
            return
        with tempfile.TemporaryDirectory(prefix="hpc-startup-") as directory:
            path = Path(directory) / "task.xml"
            path.write_bytes(ET.tostring(root, encoding="utf-16", xml_declaration=True))
            result = self.runner(["schtasks", "/Create", "/TN", self.name, "/XML", str(path), "/F"])
        if result.returncode:
            raise HpcMcpError("STARTUP_INSTALL_FAILED", "Could not register current-user startup task")

    def verify(self, commands):
        """Read-only ownership check before a multi-resource setup transaction."""
        return self._verify_existing(self._existing(), commands)

    @staticmethod
    def _verify_existing(existing, commands):
        """Validate the observed task without another query before mutation."""
        if existing is None:
            if None in commands:
                return False
        elif any(command is not None
                and existing == (command[0], subprocess.list2cmdline(command[1:]))
                for command in commands):
            return True
        raise HpcMcpError("STARTUP_CONFLICT", "The startup command changed outside setup; it was preserved")

    def uninstall(self, *, allowed=None):
        """Remove startup, checking any transaction commands against the final read."""
        existing = self._existing()
        if allowed is not None:
            self._verify_existing(existing, allowed)
        if existing is not None:
            result = self.runner(["schtasks", "/Delete", "/TN", self.name, "/F"])
            if result.returncode:
                raise HpcMcpError("STARTUP_REMOVE_FAILED", "Could not remove managed startup task")
