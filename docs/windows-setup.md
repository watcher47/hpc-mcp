# Windows local setup and rollback

`hpc-setup install` and `uninstall`, plus the agent startup commands, default to
read-only plans. Only `--apply` changes Codex registration, the current user's
Task Scheduler entry, or the installed Skill. These commands do not require an
administrator account. `uv` must be available on PATH. Codex uses PATH first;
if absent there on Windows, it discovers the current user's installed native
desktop executable under LocalAppData/OpenAI/Codex/bin, including the Codex
MSIX LocalCache backing. The newest executable modification time wins, with a
stable path tie-break. This discovery never starts a model.

Keep `servers.json` in the user-local configuration directory, outside Git and
OneDrive. After editing its template, run:

```powershell
hpc-setup validate-config
hpc-setup doctor
hpc-setup doctor --check-ssh
hpc-setup install --source .
```

`doctor` checks the installed Codex app-server schema without starting a model
turn. `--check-ssh` additionally authenticates each configured server and checks
`sbatch`, `squeue`, and `sacct`. It can establish local TOFU trust after successful
authentication, but submits no job and changes no remote file. Results contain
capability booleans and aliases, never credentials or raw SSH diagnostics.

After reviewing the plan, deployment is explicit:

```powershell
hpc-setup install --source . --apply
# Alternatively, use a prebuilt wheel:
hpc-setup install --wheel PATH_TO_NEW_WHEEL --rollback-wheel PATH_TO_OLD_WHEEL --apply
```

Apply validates the real configuration and current Codex schema, checks SSH and
Slurm, builds a candidate when `--source` is supplied, archives its SHA-256-named
wheel, installs into a separate release virtual environment, and performs an
isolated Python import smoke test and all three CLI `--help` checks before registration. `uv` uses copy mode for
compatibility with Windows cache/filesystem combinations. Existing environments
are preserved. A wheel missing the packaged `hpc-slurm-workflow/SKILL.md` fails
with `SKILL_ASSET_MISSING` before registration. The release includes the generic
skill asset.

The installer creates one `hpc-mcp` STDIO registration, one SID-scoped login task,
and one `hpc-slurm-workflow/SKILL.md` copy. Repeating an install reuses these names.
The task uses `InteractiveToken`, `LeastPrivilege`, no stored password, no runtime
limit, and a quoted `pythonw.exe` command. It starts at the user's next login.
To begin monitoring in the current session, follow the validated owned-task start
in [the guided installation contract](install-with-codex.md). Check active agent
status and identity first; never start a conflicting interpreter.
The process lock is released by Windows even after a crash; stale lock files do
not prevent restart. SQLite recovers watches and interrupted handoffs.

On Windows, immutable release wheels and virtual environments are retained under
`Path.home() / '.hpc-codex-mcp' / 'releases' / <full-wheel-SHA-256>` (normally
`%USERPROFILE%\.hpc-codex-mcp\releases\<full-wheel-SHA-256>`). This shorter runtime
path avoids MSIX expansion of AppData paths and the resulting launcher/import
path-length failures. Installation metadata, journals, rollback artifacts, and
SQLite databases use `Path.home() / '.hpc-codex-mcp' / 'state'` (normally
`%USERPROFILE%\.hpc-codex-mcp\state`). This shared non-AppData location prevents
the desktop MSIX overlay and Task Scheduler from seeing different state.
Configuration still defaults to `user_config_path('hpc-codex-mcp', appauthor=False)`;
ensure that the physical `servers.json` is accessible from both desktop and
scheduler contexts. The package does not copy or automatically migrate existing
configuration or databases. Preserve prior databases with a controlled backup
before deployment. Explicit `AppPaths` remain unchanged. Installer state overrides
retain `state/releases`, as do non-Windows defaults.
A pending journal makes an interrupted installation
recoverable on the next explicit install. Immediate registration failures restore
the prior supported MCP command and Skill. An existing MCP registration with
environment variables, an HTTP transport, custom working directory, or unsupported
options is rejected before replacement; this preserves configurations that cannot
be faithfully restored through the supported CLI adapter. A startup entry without
this package's ownership marker is never replaced or removed.

Task Scheduler may omit `RunLevel` because it defaults to least privilege and
may render the logon user as `DOMAIN\username`. The ownership check accepts
these forms only when native Windows SID lookup proves the same current user;
explicit elevation, empty/duplicate/nested security fields, any-user triggers,
and changed commands remain conflicts. See Microsoft's
[LogonTrigger.UserId contract](https://learn.microsoft.com/en-us/windows/win32/taskschd/logontrigger-userid)
and [principalType schema](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-principaltype-complextype).

`--rollback-wheel` supplies an integrity-checked rollback artifact for an
installer-managed registration and release. It does not authorize takeover of an
external or otherwise unmanaged registration: on `--apply`, such a registration
is rejected before release staging, without serializing or replacing its data.
Already managed wheels and release directories are retained automatically. No
credential file is copied into release or rollback metadata.

```powershell
hpc-agent install-startup          # plan only
hpc-agent install-startup --apply  # current installed interpreter
hpc-agent uninstall-startup --apply
hpc-setup uninstall               # plan only
hpc-setup uninstall --apply
```

A normal uninstall with no prior managed state removes the MCP registration,
startup task, and unmodified Skill owned by this installation. When complete
format-3 metadata records a prior managed state, uninstall instead restores that
state coherently: its registration and valid metadata, plus its startup task and
Skill when each existed before the upgrade. Incomplete format-2 upgrade history is
refused before mutation rather than partially restored. Configuration, all SQLite
databases, job watches, events, and retained wheels remain in place.

Uninstall also refuses mutation if the managed registration, startup task, or
Skill has been independently changed. These failures use concise, generic
diagnostics that do not include registration contents or secrets. Running
`hpc-setup uninstall` without `--apply` remains a read-only plan. Uninstalling
startup does not terminate a running agent; stop that process separately when
desired.
