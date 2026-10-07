import hashlib
import os
from pathlib import Path

from hpc_mcp.project.slug import project_slug


def test_project_slug_normalizes_the_project_basename(tmp_path: Path) -> None:
    local_root = tmp_path / "Café_Project 01"

    assert project_slug(local_root) == "cafe-project-01"


def test_project_slug_uses_path_hash_when_basename_has_no_ascii_name(
    tmp_path: Path,
) -> None:
    local_root = tmp_path / "研究"
    normalized_path = os.path.normcase(str(local_root.resolve()))
    expected_hash = hashlib.sha256(normalized_path.encode("utf-8")).hexdigest()[:8]

    assert project_slug(local_root) == f"project-{expected_hash}"
