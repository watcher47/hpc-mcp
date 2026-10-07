from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import asyncssh

from hpc_mcp.errors import HpcMcpError


class HostTrustStore:
    """Atomic, process-safe persistence for TOFU fingerprints."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def lookup(self, alias: str, host: str, port: int) -> str | None:
        try:
            with closing(self._connect()) as connection, connection:
                row = connection.execute(
                    "SELECT host, port, fingerprint FROM host_trust WHERE alias = ?",
                    (alias,),
                ).fetchone()
        except sqlite3.Error:
            raise HpcMcpError(
                "HOST_TRUST_FAILED", "Could not read SSH host trust state"
            ) from None
        if row is None:
            return None
        if tuple(row[:2]) != (host, port):
            raise HpcMcpError(
                "HOST_KEY_CHANGED", "SSH endpoint changed for server profile"
            )
        return str(row[2])

    def record_authenticated(
        self, alias: str, host: str, port: int, fingerprint: str
    ) -> None:
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT host, port, fingerprint FROM host_trust WHERE alias = ?",
                    (alias,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO host_trust(alias, host, port, fingerprint) VALUES (?, ?, ?, ?)",
                        (alias, host, port, fingerprint),
                    )
                elif tuple(row) != (host, port, fingerprint):
                    raise HpcMcpError(
                        "HOST_KEY_CHANGED", "SSH host key changed for server profile"
                    )
                connection.commit()
        except HpcMcpError:
            raise
        except sqlite3.Error:
            raise HpcMcpError(
                "HOST_TRUST_FAILED", "Could not update SSH host trust state"
            ) from None

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._path, timeout=10, isolation_level=None)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS host_trust ("
            "alias TEXT PRIMARY KEY, host TEXT NOT NULL, port INTEGER NOT NULL, fingerprint TEXT NOT NULL)"
        )
        return connection


class TofuHostKeyVerifier(asyncssh.SSHClient):
    def __init__(self, store: HostTrustStore, alias: str, host: str, port: int) -> None:
        self._store = store
        self._alias = alias
        self._host = host
        self._port = port
        self._pending_fingerprint: str | None = None

    def validate_host_public_key(
        self, host: str, addr: str, port: int, key: Any
    ) -> bool:
        del host, addr, port
        fingerprint = str(key.get_fingerprint("sha256"))
        expected = self._store.lookup(self._alias, self._host, self._port)
        if expected is not None and expected != fingerprint:
            raise HpcMcpError(
                "HOST_KEY_CHANGED", "SSH host key changed for server profile"
            )
        self._pending_fingerprint = fingerprint
        return True

    def commit_authenticated(self) -> None:
        if self._pending_fingerprint is None:
            raise HpcMcpError(
                "HOST_KEY_VERIFICATION_FAILED", "SSH host key was not available"
            )
        self._store.record_authenticated(
            self._alias, self._host, self._port, self._pending_fingerprint
        )
