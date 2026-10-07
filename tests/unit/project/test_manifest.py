from pathlib import Path

from hpc_mcp.project.manifest import FileEntry, ProjectManifest


def test_diff_uploads_only_changed_local_files() -> None:
    previous = ProjectManifest(files={"main.py": FileEntry(3, 1, "old")})
    local = ProjectManifest(
        files={
            "main.py": FileEntry(3, 2, "new"),
            "README.md": FileEntry(4, 2, "readme"),
        }
    )
    remote = ProjectManifest(files={"main.py": FileEntry(3, 1, "old")})

    plan = previous.diff(local=local, remote=remote)

    assert plan.upload == ["README.md", "main.py"]
    assert plan.conflicts == []


def test_diff_conflicts_when_local_and_remote_both_changed() -> None:
    previous = ProjectManifest(files={"main.py": FileEntry(3, 1, "old")})
    local = ProjectManifest(files={"main.py": FileEntry(3, 2, "local")})
    remote = ProjectManifest(files={"main.py": FileEntry(3, 3, "remote")})

    plan = previous.diff(local=local, remote=remote)

    assert plan.upload == []
    assert plan.conflicts == ["main.py"]


def test_scan_hashes_regular_files_and_applies_all_ignore_rules(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / ".hpcignore").write_text("ignored.txt\nignored-dir/\n", encoding="utf-8")
    # Keep the fixture byte-for-byte stable on Windows, where text writes use
    # CRLF by default but manifests hash the actual file bytes.
    (root / "main.py").write_bytes(b"print('hello')\n")
    (root / "ignored.txt").write_text("ignored\n", encoding="utf-8")
    (root / "ignored-dir").mkdir()
    (root / "ignored-dir" / "data.bin").write_bytes(b"ignored")
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("ignored", encoding="utf-8")
    (root / "pkg" / "__pycache__").mkdir(parents=True)
    (root / "pkg" / "__pycache__" / "cache.pyc").write_bytes(b"ignored")
    (root / "pkg" / "module.pyc").write_bytes(b"ignored")

    manifest = ProjectManifest.scan(root)

    assert sorted(manifest.files) == [".hpcignore", "main.py"]
    assert manifest.files["main.py"].size == len(b"print('hello')\n")
    assert (
        manifest.files["main.py"].sha256
        == "03e693d9f2f687e0f40e36a8df7fcb4d1c22974012b7c2a55c000eb30f305824"
    )


def test_scan_does_not_allow_hpcignore_to_negate_fixed_exclusions(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / ".hpcignore").write_text(
        "!.git/config\n!pkg/__pycache__/cache.pyc\n!pkg/module.pyc\n",
        encoding="utf-8",
    )
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("secret", encoding="utf-8")
    (root / "pkg" / "__pycache__").mkdir(parents=True)
    (root / "pkg" / "__pycache__" / "cache.pyc").write_bytes(b"cache")
    (root / "pkg" / "module.pyc").write_bytes(b"bytecode")
    (root / "pkg" / "module.py").write_text("safe\n", encoding="utf-8")

    manifest = ProjectManifest.scan(root)

    assert sorted(manifest.files) == [".hpcignore", "pkg/module.py"]


def test_scan_does_not_follow_file_or_directory_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    (root / "directory-link").symlink_to(outside, target_is_directory=True)
    (root / "file-link").symlink_to(outside / "secret.txt")
    (root / "safe.txt").write_text("safe", encoding="utf-8")

    manifest = ProjectManifest.scan(root)

    assert sorted(manifest.files) == ["safe.txt"]


def test_manifest_json_round_trip_is_stable() -> None:
    manifest = ProjectManifest(
        files={
            "z.txt": FileEntry(1, 22, "z-hash"),
            "a.txt": FileEntry(2, 11, "a-hash"),
        }
    )

    encoded = manifest.to_json()

    assert ProjectManifest.from_json(encoded) == manifest
    assert encoded.index('"a.txt"') < encoded.index('"z.txt"')
