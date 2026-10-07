"""Locate Codex without a shell or a dependency on desktop process PATH."""
from __future__ import annotations

import os
import shutil
from pathlib import Path


def resolve_codex_executable(*, platform=None, local_app_data=None) -> str:
    """Keep PATH priority; use only current-user standard desktop locations.

    Returning the normal command when absent preserves callers' existing safe
    CODEX_UNAVAILABLE handling at their subprocess boundary.
    """
    selected = shutil.which("codex")
    if selected:
        return selected
    if (platform or os.name) != "nt":
        return "codex"
    local = Path(local_app_data or os.environ.get("LOCALAPPDATA")
                 or Path.home() / "AppData" / "Local")
    roots = [local / "OpenAI" / "Codex" / "bin"]
    # MSIX maps LocalAppData writes into its package's LocalCache backing.
    # Enumerate only OpenAI Codex packages, never unrelated app data/disks.
    roots.extend(local.glob("Packages/OpenAI.Codex_*/LocalCache/Local/OpenAI/Codex/bin"))
    candidates = []
    for root in roots:
        for path in root.glob("*/codex.exe"):
            try:
                if path.is_file():
                    candidates.append((path.stat().st_mtime_ns, str(path)))
            except OSError:
                continue
    if not candidates:
        return "codex"
    return min(candidates, key=lambda item: (-item[0], item[1].casefold(), item[1]))[1]
