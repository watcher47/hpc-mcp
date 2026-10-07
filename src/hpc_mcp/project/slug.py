import hashlib
import os
import re
import unicodedata
from pathlib import Path


def normalized_local_root(local_root: Path) -> str:
    return os.path.normcase(str(local_root.expanduser().resolve()))


def path_hash(local_root: Path) -> str:
    normalized_path = normalized_local_root(local_root)
    return hashlib.sha256(normalized_path.encode("utf-8")).hexdigest()[:8]


def project_slug(local_root: Path) -> str:
    ascii_name = (
        unicodedata.normalize("NFKD", local_root.name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-")
    return slug or f"project-{path_hash(local_root)}"
