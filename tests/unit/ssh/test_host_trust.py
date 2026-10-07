from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.trust import HostTrustStore, TofuHostKeyVerifier


class FakeHostKey:
    def __init__(self, fingerprint: str) -> None:
        self._fingerprint = fingerprint

    def get_fingerprint(self, hash_name: str = "sha256") -> str:
        assert hash_name == "sha256"
        return self._fingerprint


def _verifier(store: HostTrustStore, fingerprint: str) -> TofuHostKeyVerifier:
    verifier = TofuHostKeyVerifier(store, "default", "compute.invalid", 22)
    assert verifier.validate_host_public_key(
        "compute.invalid", "192.0.2.10", 22, FakeHostKey(fingerprint)
    )
    return verifier


def test_first_key_is_not_persisted_before_authentication(tmp_path: Path) -> None:
    store = HostTrustStore(tmp_path / "host-trust.sqlite3")

    _verifier(store, "SHA256:first")

    assert store.lookup("default", "compute.invalid", 22) is None


def test_first_key_is_persisted_only_after_authentication(tmp_path: Path) -> None:
    store = HostTrustStore(tmp_path / "host-trust.sqlite3")
    verifier = _verifier(store, "SHA256:first")

    verifier.commit_authenticated()

    assert store.lookup("default", "compute.invalid", 22) == "SHA256:first"


def test_matching_key_is_accepted_and_changed_key_is_rejected(tmp_path: Path) -> None:
    store = HostTrustStore(tmp_path / "host-trust.sqlite3")
    _verifier(store, "SHA256:first").commit_authenticated()

    _verifier(store, "SHA256:first").commit_authenticated()
    changed = TofuHostKeyVerifier(store, "default", "compute.invalid", 22)
    with pytest.raises(HpcMcpError) as error:
        changed.validate_host_public_key(
            "compute.invalid", "192.0.2.10", 22, FakeHostKey("SHA256:changed")
        )

    assert error.value.code == "HOST_KEY_CHANGED"
    assert store.lookup("default", "compute.invalid", 22) == "SHA256:first"


def test_alias_endpoint_change_is_rejected_before_authentication(
    tmp_path: Path,
) -> None:
    store = HostTrustStore(tmp_path / "host-trust.sqlite3")
    _verifier(store, "SHA256:first").commit_authenticated()

    moved = TofuHostKeyVerifier(store, "default", "other.invalid", 22)
    with pytest.raises(HpcMcpError) as error:
        moved.validate_host_public_key(
            "other.invalid", "192.0.2.21", 22, FakeHostKey("SHA256:first")
        )

    assert error.value.code == "HOST_KEY_CHANGED"


def test_atomic_first_write_allows_only_one_concurrent_fingerprint(
    tmp_path: Path,
) -> None:
    path = tmp_path / "host-trust.sqlite3"

    def record(fingerprint: str) -> str:
        store = HostTrustStore(path)
        try:
            store.record_authenticated("default", "compute.invalid", 22, fingerprint)
            return "stored"
        except HpcMcpError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(record, ["SHA256:first", "SHA256:other"]))

    assert sorted(outcomes) == ["HOST_KEY_CHANGED", "stored"]
    assert HostTrustStore(path).lookup("default", "compute.invalid", 22) in {
        "SHA256:first",
        "SHA256:other",
    }


def test_lookup_closes_database_connection(tmp_path: Path, monkeypatch) -> None:
    store = HostTrustStore(tmp_path / "host-trust.sqlite3")

    class Cursor:
        def fetchone(self):
            return None

    class Connection:
        closed = False

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, *args: object) -> Cursor:
            return Cursor()

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(store, "_connect", lambda: connection)

    assert store.lookup("default", "compute.invalid", 22) is None
    assert connection.closed is True
