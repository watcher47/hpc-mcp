"""Native single-job cancellation with durable audit and uncertain-outcome fencing."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.slurm import cancel_once
from hpc_mcp.ssh.policy import CommandPolicy, RemoteCommand


MESSAGES = {
    "JOB_NOT_OWNED": "Job is not owned by the authenticated remote user",
    "SLURM_QUERY_FAILED": "Could not verify the job with Slurm; cancellation was not dispatched",
    "SLURM_QUERY_INVALID": "Slurm ownership response was invalid; cancellation was not dispatched",
    "CANCELLATION_UNCERTAIN": "Cancellation outcome is uncertain; inspect the job before any new request",
}


class CancellationStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        with self._connect() as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS cancellations (
                request_id TEXT PRIMARY KEY, server_alias TEXT NOT NULL, job_id TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, response TEXT)""")

    def _connect(self):
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def get(self, request_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM cancellations WHERE request_id=?", (request_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["response"] = json.loads(value["response"]) if value["response"] else None
        return value

    def begin(self, request_id: str, server_alias: str, job_id: str) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            inserted = connection.execute(
                "INSERT OR IGNORE INTO cancellations VALUES (?, ?, ?, ?, ?, NULL)",
                (request_id, server_alias, job_id, now, now),
            ).rowcount == 1
            row = connection.execute("SELECT server_alias,job_id FROM cancellations WHERE request_id=?", (request_id,)).fetchone()
            if (row["server_alias"], row["job_id"]) != (server_alias, job_id):
                raise HpcMcpError("CANCELLATION_REQUEST_CONFLICT", "Request UUID belongs to a different target")
        return inserted

    def finish(self, request_id: str, response: dict) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE cancellations SET response=?,updated_at=? WHERE request_id=?",
                               (json.dumps(response, sort_keys=True), datetime.now(timezone.utc).isoformat(), request_id))


class SlurmCancelService:
    def __init__(self, registry, ssh, store: CancellationStore) -> None:
        self._registry, self._ssh, self._store = registry, ssh, store

    async def cancel_job(self, job_id: str, request_id: UUID, server_alias: str | None = None) -> dict:
        request = str(request_id)
        try:
            cancel_once.validate_target(job_id, request)
        except (ValueError, TypeError, AttributeError):
            raise HpcMcpError("INVALID_CANCELLATION_REQUEST", "Expected one job ID and a request UUID") from None
        # An explicit configured alias also permits the user's jobs submitted outside this MCP.
        if server_alias is None:
            registered = self._registry.find_job(job_id)
            if registered is None:
                raise HpcMcpError("JOB_NOT_REGISTERED", "Job is not registered; select an existing server alias explicitly")
            alias = registered.server_alias
        else:
            alias = server_alias
        client = self._ssh(alias)  # Resolve existing config; never accept host/credentials as input.
        cwd = await client.realpath(".")
        try:
            fresh = self._store.begin(request, alias, job_id)
            if not fresh:
                previous = self._store.get(request)["response"]
                return self._result(previous or self._uncertain(request, alias, job_id))
        except (OSError, sqlite3.Error):
            raise HpcMcpError("CANCELLATION_AUDIT_FAILED", "Could not record intent; cancellation was not dispatched") from None
        command = RemoteCommand(
            "python3", ["-I", "-", "cancel", "--job-id", job_id, "--request-id", request], cwd,
            timeout_seconds=30, standard_input=Path(cancel_once.__file__).read_bytes(),
        )
        try:
            raw = await client.run(CommandPolicy().validate(command))
            response = json.loads(raw.stdout)
            if raw.exit_status != 0 or raw.truncated or not self._valid(response, request, job_id):
                raise ValueError("Invalid helper response")
            response["server_alias"] = alias
        except (HpcMcpError, OSError, ValueError, TypeError):
            response = self._uncertain(request, alias, job_id)
        try:
            self._store.finish(request, response)
        except (OSError, sqlite3.Error):
            # The durable pending intent remains a fence if saving the outcome fails.
            return self._result(self._uncertain(request, alias, job_id))
        return self._result(response)

    @staticmethod
    def _valid(value, request, job_id) -> bool:
        if not isinstance(value, dict) or value.get("request_id") != request or value.get("job_id") != job_id:
            return False
        status, code = value.get("status"), value.get("error_code")
        if status not in {"cancellation_requested", "not_active", "rejected", "uncertain"}:
            return False
        if (code not in MESSAGES if status in {"rejected", "uncertain"} else code is not None):
            return False
        actor, owner, states = value.get("actor_uid"), value.get("owner_uid"), value.get("previous_states")
        return (type(actor) is int and actor >= 0 and (owner is None or type(owner) is int)
                and (status != "cancellation_requested" or owner == actor)
                and isinstance(states, list) and len(states) <= 64
                and all(isinstance(state, str) and len(state) <= 64 for state in states))

    @staticmethod
    def _uncertain(request, alias, job_id):
        return dict(request_id=request, server_alias=alias, job_id=job_id,
                    status="uncertain", error_code="CANCELLATION_UNCERTAIN")

    @staticmethod
    def _result(response):
        code = response.get("error_code")
        if code:
            raise HpcMcpError(code, MESSAGES[code], details=response)
        return response
