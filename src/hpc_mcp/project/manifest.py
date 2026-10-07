from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pathspec


_HASH_CHUNK_SIZE = 1024 * 1024
_FIXED_EXCLUSIONS = (
    ".git/**",
    "**/__pycache__/**",
    "**/*.pyc",
)
_FIXED_IGNORE_SPEC = pathspec.PathSpec.from_lines(
    "gitwildmatch",
    _FIXED_EXCLUSIONS,
)


@dataclass(frozen=True, slots=True)
class FileEntry:
    size: int
    mtime_ns: int
    sha256: str

    def to_dict(self) -> dict[str, int | str]:
        return {
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FileEntry:
        return cls(
            size=int(value["size"]),
            mtime_ns=int(value["mtime_ns"]),
            sha256=str(value["sha256"]),
        )


@dataclass(frozen=True, slots=True)
class SyncPlan:
    upload: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    local_manifest: ProjectManifest | None = None


@dataclass(frozen=True, slots=True)
class ProjectManifest:
    files: dict[str, FileEntry] = field(default_factory=dict)

    @classmethod
    def scan(cls, root: Path) -> ProjectManifest:
        root = Path(root)
        if root.is_symlink() or not root.is_dir():
            raise ValueError("project root must be a non-symlink directory")

        ignore_spec = _ignore_spec(root)
        files: dict[str, FileEntry] = {}
        for current, directories, filenames in os.walk(root, followlinks=False):
            current_path = Path(current)
            kept_directories: list[str] = []
            for directory in directories:
                path = current_path / directory
                relative = path.relative_to(root).as_posix()
                if path.is_symlink() or _fixed_directory(relative):
                    continue
                if _is_ignored(relative, ignore_spec, is_directory=True):
                    continue
                kept_directories.append(directory)
            directories[:] = kept_directories

            for filename in filenames:
                path = current_path / filename
                relative = path.relative_to(root).as_posix()
                if _is_ignored(relative, ignore_spec):
                    continue
                entry = _hash_regular_file(path)
                if entry is not None:
                    files[relative] = entry

        return cls(files=dict(sorted(files.items())))

    def diff(
        self,
        *,
        local: ProjectManifest,
        remote: ProjectManifest,
    ) -> SyncPlan:
        upload: list[str] = []
        conflicts: list[str] = []
        all_paths = sorted(set(self.files) | set(local.files) | set(remote.files))
        for path in all_paths:
            previous_entry = self.files.get(path)
            local_entry = local.files.get(path)
            remote_entry = remote.files.get(path)
            local_changed = not _same_content(local_entry, previous_entry)
            remote_changed = not _same_content(remote_entry, previous_entry)

            if local_changed and remote_changed and not _same_content(
                local_entry, remote_entry
            ):
                conflicts.append(path)
            elif local_changed and local_entry is not None:
                upload.append(path)

        return SyncPlan(
            upload=upload,
            conflicts=conflicts,
            local_manifest=local,
        )

    def to_json(self) -> str:
        payload = {
            "files": {
                path: entry.to_dict() for path, entry in sorted(self.files.items())
            }
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, value: str | bytes) -> ProjectManifest:
        payload = json.loads(value)
        raw_files = payload.get("files", {})
        if not isinstance(raw_files, dict):
            raise ValueError("manifest files must be an object")
        return cls(
            files={
                str(path): FileEntry.from_dict(entry)
                for path, entry in sorted(raw_files.items())
            }
        )


def _ignore_spec(root: Path) -> pathspec.PathSpec:
    patterns: list[str] = []
    ignore_path = root / ".hpcignore"
    if ignore_path.is_file() and not ignore_path.is_symlink():
        patterns.extend(ignore_path.read_text(encoding="utf-8").splitlines())
    return pathspec.PathSpec.from_lines("gitwildmatch", patterns)


def _is_ignored(
    relative: str,
    user_spec: pathspec.PathSpec,
    *,
    is_directory: bool = False,
) -> bool:
    candidate = f"{relative}/" if is_directory else relative
    return _FIXED_IGNORE_SPEC.match_file(candidate) or user_spec.match_file(candidate)


def _fixed_directory(relative: str) -> bool:
    return any(part in {".git", "__pycache__"} for part in Path(relative).parts)


def _hash_regular_file(path: Path) -> FileEntry | None:
    try:
        if path.is_symlink():
            return None
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        no_follow = getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags | no_follow)
    except (FileNotFoundError, OSError):
        return None

    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            return None
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(_HASH_CHUNK_SIZE):
                digest.update(chunk)
        return FileEntry(
            size=details.st_size,
            mtime_ns=details.st_mtime_ns,
            sha256=digest.hexdigest(),
        )
    finally:
        os.close(descriptor)


def _same_content(left: FileEntry | None, right: FileEntry | None) -> bool:
    if left is None or right is None:
        return left is right
    return left.sha256 == right.sha256
