import copy
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest


def test_scheduler_normalized_xml(managed_startup):
    startup, scheduler, command = managed_startup
    root = ET.fromstring(scheduler.xml)
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    principal = root.find(".//t:Principal", ns)
    principal.remove(principal.find("t:RunLevel", ns))
    root.find(".//t:LogonTrigger/t:UserId", ns).text = r"DOMAIN\fixture"
    startup.account_sid_lookup = lambda account: startup.sid if account == r"DOMAIN\fixture" else "S-1-5-18"
    scheduler.xml = ET.tostring(root, encoding="unicode")
    assert startup.verify([command])
    root.find(".//t:LogonTrigger/t:UserId", ns).text = r"DOMAIN\other"
    scheduler.xml = ET.tostring(root, encoding="unicode")
    with pytest.raises(HpcMcpError):
        startup.verify([command])
    root.find(".//t:LogonTrigger/t:UserId", ns).text = r"DOMAIN\fixture"
    scheduler.xml = ET.tostring(root, encoding="unicode")
    startup.install(command, allowed=[command])
    assert not any("/Create" in call for call in scheduler.calls)
    startup.uninstall(allowed=[command])
    assert scheduler.xml == ""

from hpc_mcp.errors import HpcMcpError


def test_single_instance_lock_released_after_error(tmp_path):
    from hpc_mcp.setup.windows import InstanceLock
    path = tmp_path / "agent.lock"
    with pytest.raises(ValueError):
        with InstanceLock(path):
            with pytest.raises(HpcMcpError, match="already running"):
                with InstanceLock(path):
                    pass
            raise ValueError()
    with InstanceLock(path):
        assert path.exists()


def test_single_instance_lock_blocks_another_process(tmp_path):
    from hpc_mcp.setup.windows import InstanceLock
    path = tmp_path / "agent.lock"
    code = "from pathlib import Path; from hpc_mcp.setup.windows import InstanceLock; InstanceLock(Path(__import__('sys').argv[1])).__enter__()"
    with InstanceLock(path):
        result = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True)
    assert result.returncode != 0
    assert b"already running" in result.stderr


def test_task_xml_and_idempotent_registration_are_current_user_only(tmp_path):
    from hpc_mcp.setup.windows import WindowsStartup
    tasks = {}
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if "/Query" in args:
            return SimpleNamespace(returncode=0, stdout=tasks.get(args[args.index("/TN") + 1], ""))
        if "/Create" in args:
            tasks[args[args.index("/TN") + 1]] = Path(args[args.index("/XML") + 1]).read_text(encoding="utf-16")
        if "/Delete" in args:
            tasks.pop(args[args.index("/TN") + 1], None)
        return SimpleNamespace(returncode=0, stdout="")
    startup = WindowsStartup(r"S-1-5-21-fixture", runner=run, platform="nt")
    command = [r"C:\local tools\pythonw.exe", "-m", "hpc_mcp.agent.cli", "--config-file", r"C:\local & data\servers.json", "run"]
    assert startup.verify([None]) is False
    startup.install(command)
    startup.install(command)
    assert len(tasks) == 1
    xml = ET.fromstring(next(iter(tasks.values())))
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    assert xml.find(".//t:RunLevel", ns).text == "LeastPrivilege"
    assert xml.find(".//t:LogonType", ns).text == "InteractiveToken"
    assert xml.find(".//t:LogonTrigger/t:UserId", ns).text == "S-1-5-21-fixture"
    assert xml.find(".//t:Command", ns).text == command[0]
    assert xml.find(".//t:Arguments", ns).text == subprocess.list2cmdline(command[1:])
    assert len([c for c in calls if "/Create" in c]) == 1
    assert startup.verify([command]) is True
    with pytest.raises(HpcMcpError):
        startup.verify([["different", "command"]])
    original = tasks[startup.name]
    actions = xml.find(".//t:Actions", ns)
    ET.SubElement(actions, "{http://schemas.microsoft.com/windows/2004/02/mit/task}Exec")
    tasks[startup.name] = ET.tostring(xml, encoding="unicode")
    with pytest.raises(HpcMcpError):
        startup.verify([command])
    tasks[startup.name] = original
    startup.uninstall()
    startup.uninstall()
    assert not tasks


def test_scheduler_refuses_foreign_entry_and_sanitizes_failures():
    from hpc_mcp.setup.windows import WindowsStartup
    def run(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="<Task><Description>foreign</Description></Task>")
    startup = WindowsStartup("fixture", runner=run, platform="nt")
    for operation in (lambda: startup.install(["python", "run"]), startup.uninstall):
        with pytest.raises(HpcMcpError) as error:
            operation()
        assert error.value.code == "STARTUP_CONFLICT"


@pytest.fixture
def managed_startup():
    """Exercise the actual adapter with only its scheduler boundary replaced."""
    from hpc_mcp.setup.windows import WindowsStartup
    scheduler = SimpleNamespace(xml="", calls=[])

    def run(args):
        assert args[0] == "schtasks"
        scheduler.calls.append(args)
        if "/Query" in args:
            assert "/TN" in args and "/XML" in args
            return SimpleNamespace(returncode=0, stdout=scheduler.xml)
        if "/Create" in args:
            scheduler.xml = Path(args[args.index("/XML") + 1]).read_text(encoding="utf-16")
        elif "/Delete" in args:
            scheduler.xml = ""
        else:
            raise AssertionError(f"Unexpected scheduler action: {args}")
        return SimpleNamespace(returncode=0, stdout="")

    startup = WindowsStartup("S-1-5-21-fixture", runner=run, platform="nt",
                             account_sid_lookup=lambda account: None)
    command = [r"C:\fixture tools\pythonw.exe", "-m", "hpc_mcp.agent.cli", "run"]
    startup.install(command, allowed=[None])
    assert startup.verify([command]) is True
    scheduler.calls.clear()
    return startup, scheduler, command


@pytest.mark.parametrize("operation", ["verify", "install", "uninstall"])
@pytest.mark.parametrize("path,change,value", [
    ("Principals/Principal/UserId", "text", "S-1-5-18"),
    ("Principals/Principal/LogonType", "text", "ServiceAccount"),
    ("Principals/Principal/RunLevel", "text", "HighestAvailable"),
    ("Principals/Principal/RunLevel", "text", ""),
    ("Principals/Principal/RunLevel", "text", "   "),
    ("Triggers/LogonTrigger/UserId", "text", ""),
    ("Triggers/LogonTrigger/UserId", "text", r"DOMAIN\foreign"),
    ("Triggers/LogonTrigger/UserId", "text", "S-1-5-18"),
    ("RegistrationInfo/Description", "text", "foreign-marker-private-text"),
    ("Actions", "context", "Foreign"),
    ("Actions", "context", ""),
    ("Actions", "context", "   "),
    ("Actions", "missing-context", None),
    ("Principals/Principal", "missing-id", None),
    ("Principals/Principal", "foreign-selected-principal", None),
    ("Actions", "foreign-root", None),
    ("Actions", "malformed-xml", None),
    *[(path, change, None) for path in (
        "Actions", "Principals", "Principals/Principal", "Triggers",
        "Triggers/LogonTrigger", "RegistrationInfo",
        "Principals/Principal/UserId", "Principals/Principal/LogonType",
        "Principals/Principal/RunLevel", "Triggers/LogonTrigger/UserId",
        "RegistrationInfo/Description",
        ) for change in ("missing", "duplicate", "nested")
          if not (path == "Principals/Principal/RunLevel" and change == "missing")],
    *[(path, "child", None) for path in (
        "Principals/Principal/UserId", "Principals/Principal/LogonType",
        "Principals/Principal/RunLevel", "Triggers/LogonTrigger/UserId",
        "RegistrationInfo/Description",
    )],
])
def test_task_identity_conflicts_are_preserved_at_each_final_read(managed_startup, operation, path, change, value):
    from hpc_mcp.setup.windows import NS
    startup, scheduler, command = managed_startup
    root = ET.fromstring(scheduler.xml)
    ns = {"t": NS}
    parts = path.split("/")
    node = root.find("/".join(f"t:{part}" for part in parts), ns)
    parent = root.find("/".join(f"t:{part}" for part in parts[:-1]), ns) if len(parts) > 1 else root
    if change == "text":
        node.text = value
    elif change == "context":
        node.set("Context", value)
    elif change == "missing-context":
        del node.attrib["Context"]
    elif change == "missing-id":
        del node.attrib["id"]
    elif change == "foreign-root":
        root.tag = f"{{{NS}}}Unrelated"
    elif change == "malformed-xml":
        pass
    elif change == "foreign-selected-principal":
        foreign = copy.deepcopy(node)
        foreign.set("id", "Foreign")
        foreign.find("t:UserId", ns).text = "S-1-5-18"
        parent.append(foreign)
        root.find("t:Actions", ns).set("Context", "Foreign")
    elif change == "duplicate":
        parent.append(copy.deepcopy(node))
    elif change == "child":
        ET.SubElement(node, f"{{{NS}}}Unexpected")
    else:
        parent.remove(node)
        if change == "nested":
            ET.SubElement(parent, f"{{{NS}}}Unexpected").append(node)
    original = scheduler.xml = ET.tostring(root, encoding="unicode")
    if change == "malformed-xml":
        original = scheduler.xml = original[:-1]

    with pytest.raises(HpcMcpError) as error:
        if operation == "verify":
            startup.verify([command])
        elif operation == "install":
            startup.install(command, allowed=[command])
        else:
            startup.uninstall(allowed=[command])

    assert error.value.code == "STARTUP_CONFLICT"
    assert "foreign-marker-private-text" not in str(error.value)
    assert len(scheduler.calls) == 1 and "/Query" in scheduler.calls[0]
    assert scheduler.xml == original


def test_canonical_principal_stays_idempotent_and_can_be_removed(managed_startup):
    startup, scheduler, command = managed_startup
    original = scheduler.xml
    assert startup.verify([command]) is True
    startup.install(command, allowed=[command])
    assert scheduler.xml == original
    assert all("/Query" in args for args in scheduler.calls)
    startup.uninstall(allowed=[command])
    startup.uninstall(allowed=[None])
    assert scheduler.xml == ""
    assert sum("/Delete" in args for args in scheduler.calls) == 1


def test_actions_context_selects_the_execution_principal(managed_startup):
    from hpc_mcp.setup.windows import NS
    startup, scheduler, command = managed_startup
    root = ET.fromstring(scheduler.xml)
    ns = {"t": NS}
    principals = root.find("t:Principals", ns)
    selected = copy.deepcopy(principals[0])
    selected.set("id", "Selected")
    principals[0].find("t:UserId", ns).text = "S-1-5-18"
    principals.append(selected)
    root.find("t:Actions", ns).set("Context", "Selected")
    original = scheduler.xml = ET.tostring(root, encoding="unicode")
    assert startup.verify([command]) is True
    startup.install(command, allowed=[command])
    assert scheduler.xml == original
    assert all("/Query" in args for args in scheduler.calls)


@pytest.mark.parametrize("operation", ["verify", "install", "uninstall"])
@pytest.mark.parametrize("path,change", [
    ("Actions/Exec", "registration-shadow"),
    *[(f"Actions/Exec/{field}", change) for field in ("Command", "Arguments")
      for change in ("foreign-text", "missing", "duplicate", "nested", "child",
                     "nested-text", "foreign-namespace")],
    ("Actions/Exec/Command", "empty"),
    ("Actions/Exec/Command", "blank"),
    ("Actions/Exec", "com-handler"),
    ("Actions/Exec", "extra-com-handler"),
    ("Actions/Exec", "duplicate"),
    ("Actions/Exec", "nested"),
    ("Actions/Exec", "foreign-namespace"),
    *[(path, change) for path in (
        "RegistrationInfo/Description", "Triggers/LogonTrigger/UserId",
        "Principals/Principal/UserId", "Principals/Principal/LogonType",
        "Principals/Principal/RunLevel",
      ) for change in ("hidden-extra", "misplaced-extra", "shadow-foreign", "shadow-missing")],
    ("RegistrationInfo", "hidden-extra"),
    ("Triggers", "hidden-extra"),
    ("Triggers/LogonTrigger", "hidden-extra"),
    ("Triggers/LogonTrigger", "extra-time-trigger"),
    ("Principals", "hidden-extra"),
    ("Principals/Principal", "hidden-extra"),
    ("Actions", "hidden-extra"),
])
def test_task_xml_structure_conflicts_preserve_every_final_read(managed_startup, operation, path, change):
    from hpc_mcp.setup.windows import NS
    startup, scheduler, command = managed_startup
    root = ET.fromstring(scheduler.xml)
    ns = {"t": NS}
    parts = path.split("/")
    node = root.find("/".join(f"t:{part}" for part in parts), ns)
    parent = root.find("/".join(f"t:{part}" for part in parts[:-1]), ns) if len(parts) > 1 else root
    if change == "registration-shadow":
        registration = root.find("t:RegistrationInfo", ns)
        for field in ("Command", "Arguments"):
            direct = node.find(f"t:{field}", ns)
            registration.append(copy.deepcopy(direct))
            direct.text = "private-foreign-exec-value"
    elif change in ("foreign-text", "empty", "blank"):
        node.text = {"foreign-text": "private-foreign-exec-value", "empty": "", "blank": "   "}[change]
    elif change == "duplicate":
        parent.append(copy.deepcopy(node))
    elif change in ("missing", "nested"):
        parent.remove(node)
        if change == "nested":
            ET.SubElement(parent, f"{{{NS}}}Unexpected").append(node)
    elif change in ("child", "nested-text"):
        child = ET.SubElement(node, f"{{{NS}}}Unexpected")
        if change == "child":
            child.tail = "private-injected-tail"
        else:
            child.text, node.text = node.text, None
    elif change == "foreign-namespace":
        node.tag = parts[-1]
    elif change == "com-handler":
        node.tag = f"{{{NS}}}ComHandler"
    elif change in ("extra-com-handler", "extra-time-trigger"):
        ET.SubElement(parent, f"{{{NS}}}{'ComHandler' if change == 'extra-com-handler' else 'TimeTrigger'}")
    else:
        # Preserve a hidden canonical value alongside a valid/foreign/missing
        # direct field, including fields outside their canonical containers.
        shadow = copy.deepcopy(node)
        if change == "misplaced-extra":
            root.append(shadow)
        else:
            container = parent if change == "hidden-extra" else root.find("t:RegistrationInfo", ns)
            ET.SubElement(container, f"{{{NS}}}Unexpected").append(shadow)
        if change == "shadow-foreign":
            node.text = "private-foreign-identity-value"
        elif change == "shadow-missing":
            parent.remove(node)
    original = scheduler.xml = ET.tostring(root, encoding="unicode")

    with pytest.raises(HpcMcpError) as error:
        if operation == "verify":
            startup.verify([command])
        elif operation == "install":
            startup.install(command, allowed=[command])
        else:
            startup.uninstall(allowed=[command])

    assert error.value.code == "STARTUP_CONFLICT"
    assert "private-" not in str(error.value)
    assert len(scheduler.calls) == 1 and "/Query" in scheduler.calls[0]
    assert scheduler.xml == original


@pytest.mark.parametrize("command", [
    [r"C:\fixture tools\pythonw.exe"],
    [r"C:\fixture tools\pythonw.exe", "-m", "hpc_mcp.agent.cli", "run"],
])
def test_canonical_command_lifecycle_including_no_arguments(managed_startup, command):
    startup, scheduler, prior = managed_startup
    startup.install(command, allowed=[prior])
    original = scheduler.xml
    scheduler.calls.clear()
    assert startup.verify([command]) is True
    startup.install(command, allowed=[command])
    assert scheduler.xml == original
    assert all("/Query" in args for args in scheduler.calls)

    upgraded = [r"C:\fixture upgrade\pythonw.exe", "--fixture-upgrade"]
    startup.install(upgraded, allowed=[command])
    assert startup.verify([upgraded]) is True
    startup.install(command, allowed=[upgraded])
    assert startup.verify([command]) is True
    startup.uninstall(allowed=[command])
    startup.uninstall(allowed=[None])
    assert scheduler.xml == ""
    assert sum("/Create" in args for args in scheduler.calls) == 2
    assert sum("/Delete" in args for args in scheduler.calls) == 1


def test_startup_rejects_non_windows():
    from hpc_mcp.setup.windows import WindowsStartup
    with pytest.raises(HpcMcpError) as error:
        WindowsStartup("fixture", platform="posix").install(["python", "run"])
    assert error.value.code == "PLATFORM_UNSUPPORTED"


def test_command_runner_decodes_unicode_without_locale_tracebacks():
    from hpc_mcp.setup import windows
    result = windows.run_command([sys.executable, "-c", "import sys; sys.stdout.buffer.write(bytes.fromhex('e29883'))"])
    assert result.returncode == 0
    assert result.stdout == "\u2603"
