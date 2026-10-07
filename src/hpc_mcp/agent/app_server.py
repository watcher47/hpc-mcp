"""Bounded Codex JSONL client, checked against the installed CLI's schema.

No protocol or child diagnostic text is included in public exceptions. Schema
generation is local and model-free; callers must only invoke it for diagnostics
or when a terminal event is ready. No real thread is touched by generation.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError, ValidationError
from referencing.exceptions import Unresolvable

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.agent.store import safe_rpc
from hpc_mcp.codex_executable import resolve_codex_executable


def protocol_error(code="CODEX_PROTOCOL_INCOMPATIBLE"):
    return HpcMcpError(code, "Codex handoff is deferred; inspect hpc-agent status and use manual-recovery", retriable=True)


def _spawn_options():
    return {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}


async def close_child(process):
    """Allow EOF shutdown, then require bounded, confirmed process exit."""
    await _complete_cleanup(_close_child(process))


async def _complete_cleanup(cleanup):
    """Retain cleanup ownership if the calling task is cancelled."""
    task = asyncio.create_task(cleanup)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()  # Cleanup errors take precedence over cancellation.
    if cancelled:
        raise asyncio.CancelledError


async def _close_child(process):
    if process.stdin:
        process.stdin.close()
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), 0.5)
        except TimeoutError:
            await _force_close_child(process, timeout=2)
            return
    await _finalize_transport(process, timeout=2)


async def force_close_child(process, *, timeout=2):
    """Escalate terminate to kill, bounding both waits and preserving errors safely."""
    await _complete_cleanup(_force_close_child(process, timeout=timeout))


async def _force_close_child(process, *, timeout):
    for signal in (process.terminate, process.kill):
        if process.returncode is None:
            # A failed terminate must not prevent the stronger kill attempt.
            with suppress(OSError):
                signal()
        try:
            await asyncio.wait_for(process.wait(), timeout)
            if process.returncode is not None:
                await _finalize_transport(process, timeout=timeout)
                return
        except (TimeoutError, OSError):
            pass
    raise protocol_error("CODEX_CHILD_CLEANUP_UNCONFIRMED")


async def _finalize_transport(process, *, timeout):
    """Close unread pipes and confirm CPython's final connection_lost callback.

    asyncio Process has no public transport-close API. CPython 3.11+ clears
    BaseSubprocessTransport._loop only after all pipe disconnections and the
    final protocol callback. Unlike returncode/is_closing, this confirms release.
    Keep this compatibility dependency isolated and fail closed if unsupported.
    No shutdown output is accumulated, and only this child's transport is closed.
    """
    transport = getattr(process, "_transport", None)
    if transport is None or not hasattr(transport, "_loop"):
        raise protocol_error("CODEX_CHILD_CLEANUP_UNCONFIRMED")
    transport.close()
    async def disconnected():
        while transport._loop is not None:
            await asyncio.sleep(.001)
    try:
        await asyncio.wait_for(disconnected(), timeout)
        if process.stdin:
            with suppress(BrokenPipeError, ConnectionResetError):
                await asyncio.wait_for(process.stdin.wait_closed(), timeout)
    except (TimeoutError, OSError):
        raise protocol_error("CODEX_CHILD_CLEANUP_UNCONFIRMED") from None


class ProtocolSchema:
    """Use exact generated request schemas, not a bundled dated protocol copy."""
    required_fields = {"initialize": {"clientInfo", "capabilities"},
        "thread/read": {"threadId", "includeTurns"},
        "thread/resume": {"threadId", "cwd"},
        "turn/start": {"threadId", "input", "clientUserMessageId"}}

    def __init__(self, requests: dict, notifications: dict, *, route="legacy"):
        self.notifications = notifications
        self.methods = {}
        self.requests = {}
        try:
            if not isinstance(requests, dict) or not isinstance(notifications, dict):
                raise ValueError()
            definitions = requests.get("definitions", {})
            for branch in requests["oneOf"]:
                method = branch["properties"]["method"]["enum"][0]
                params = branch["properties"]["params"]
                if "$ref" in params:
                    params = definitions[params["$ref"].removeprefix("#/definitions/")]
                self.methods[method] = params | {"definitions": definitions}
                self.requests[method] = branch | {"definitions": definitions}
            fields_by_method = self.required_fields if route == "legacy" else {"initialize": {"clientInfo", "capabilities"}}
            if route not in {"legacy", "queue"}:
                raise ValueError()
            for method, fields in fields_by_method.items():
                if not fields <= self.methods[method].get("properties", {}).keys():
                    raise ValueError()
                Draft7Validator.check_schema(self.methods[method])
            Draft7Validator.check_schema(notifications)
            Draft7Validator(notifications).validate({"method": "initialized"})
        except (KeyError, IndexError, TypeError, ValueError, SchemaError, ValidationError, Unresolvable):
            raise protocol_error() from None
        self.supports_trigger = "turnTrigger" in self.methods.get("turn/start", {}).get("properties", {})
        if self.supports_trigger and route == "legacy":
            self.validate("turn/start", {"threadId": "schema-check", "input": [],
                "clientUserMessageId": "schema-check", "turnTrigger": "hpc-slurm-completion"})
        if route == "queue":
            from .queue_protocol import require_queue_schema
            require_queue_schema(self)

    def validate(self, method: str, params: dict, *, request_id: int = 1) -> None:
        try:
            schema = self.methods[method]
            if not params.keys() <= schema.get("properties", {}).keys():
                raise ValueError()
            Draft7Validator(schema).validate(params)
            Draft7Validator(self.requests[method]).validate({"id": request_id, "method": method, "params": params})
        except (KeyError, TypeError, ValueError, ValidationError, Unresolvable):
            raise protocol_error() from None


async def generate_schema(*, command: list[str] | None = None, timeout: float = 30,
                          max_output_bytes: int = 1048576, route: str = "legacy") -> ProtocolSchema:
    """Generate and validate the currently selected Codex executable's contract."""
    try:
        with TemporaryDirectory(prefix="hpc-codex-schema-") as directory:
            process = None
            try:
                process = await asyncio.create_subprocess_exec(*(command if command is not None else [resolve_codex_executable()]),
                    "app-server", "generate-json-schema", "--experimental", "--out", directory,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                    **_spawn_options())
                async def drain():
                    total = 0
                    while chunk := await process.stdout.read(16384):
                        total += len(chunk)
                        if total > max_output_bytes:
                            raise protocol_error("CODEX_OUTPUT_LIMIT")
                    if await process.wait() != 0:
                        raise protocol_error()
                await asyncio.wait_for(drain(), timeout)
                files = [Path(directory) / name for name in ("ClientRequest.json", "ClientNotification.json")]
                if any(file.stat().st_size > 16777216 for file in files):
                    raise protocol_error("CODEX_OUTPUT_LIMIT")
                return ProtocolSchema(*(json.loads(file.read_text(encoding="utf-8")) for file in files), route=route)
            finally:
                # Windows cannot remove a schema file while a timed-out CLI still
                # holds it open. Reap first, then leave TemporaryDirectory.
                if process:
                    await close_child(process)
    except TimeoutError:
        raise protocol_error("CODEX_TIMEOUT") from None
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise protocol_error("CODEX_UNAVAILABLE") from None


class AppServerClient:
    def __init__(self, schema: ProtocolSchema, *, command: list[str] | None = None,
                 timeout: float = 15, max_line_bytes: int = 4194304,
                 max_output_bytes: int = 16777216):
        self.schema = schema
        self.command = command if command is not None else [resolve_codex_executable(), "app-server", "--listen", "stdio://"]
        self.timeout, self.max_line_bytes, self.max_output_bytes = timeout, max_line_bytes, max_output_bytes
        self.process = None
        self._stderr_task = None
        self._output_bytes = 0
        self._limit_exceeded = False
        self._next_id = 0
        self._completed = []

    def _account(self, size):
        self._output_bytes += size
        if self._output_bytes > self.max_output_bytes:
            self._limit_exceeded = True
            raise protocol_error("CODEX_OUTPUT_LIMIT")

    async def _drain_stderr(self):
        try:
            while chunk := await self.process.stderr.read(16384):
                self._account(len(chunk))
        except HpcMcpError:
            with suppress(ProcessLookupError):
                self.process.terminate()

    async def __aenter__(self):
        try:
            self.process = await asyncio.create_subprocess_exec(*self.command,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, limit=self.max_line_bytes, **_spawn_options())
            self._stderr_task = asyncio.create_task(self._drain_stderr())
            await self.request("initialize", {"clientInfo": {"name": "hpc-agent", "version": "0.1.0"},
                                              "capabilities": {"experimentalApi": True}})
            await self._write({"method": "initialized"})
            return self
        except BaseException as error:
            await self.__aexit__(None, None, None)
            if isinstance(error, OSError):
                raise protocol_error("CODEX_UNAVAILABLE") from None
            raise

    async def __aexit__(self, *_):
        try:
            if self.process:
                await close_child(self.process)
        finally:
            if self._stderr_task:
                self._stderr_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._stderr_task

    async def force_reap(self):
        """Fallback independent of overridable normal context-manager cleanup."""
        try:
            if self.process:
                await force_close_child(self.process)
        finally:
            if self._stderr_task:
                self._stderr_task.cancel()
                await asyncio.gather(self._stderr_task, return_exceptions=True)

    async def _write(self, payload):
        raw = json.dumps(payload, ensure_ascii=True).encode() + b"\n"
        if len(raw) > self.max_line_bytes:
            raise protocol_error("CODEX_OUTPUT_LIMIT")
        self.process.stdin.write(raw)
        await self.process.stdin.drain()

    async def request(self, method: str, params: dict) -> dict:
        self._next_id += 1
        request_id = self._next_id
        self.schema.validate(method, params, request_id=request_id)
        async def exchange():
            await self._write({"id": request_id, "method": method, "params": params})
            while True:
                message = await self._read_message()
                if "method" in message:
                    if message["method"] == "turn/completed":
                        self._completed.append(message)
                    continue
                if message.get("id") != request_id:
                    raise protocol_error("CODEX_PROTOCOL_INVALID")
                if "error" in message:
                    error = message["error"]
                    safe_method, safe_code = safe_rpc(method, error.get("code") if isinstance(error, dict) else None)
                    raise HpcMcpError("CODEX_REQUEST_FAILED",
                        "Codex request failed; inspect hpc-agent diagnostics", retriable=True,
                        details={"rpc_method": safe_method, "rpc_code": safe_code})
                if not isinstance(message.get("result"), dict):
                    raise protocol_error("CODEX_PROTOCOL_INVALID")
                return message["result"]
        try:
            return await asyncio.wait_for(exchange(), self.timeout)
        except TimeoutError:
            raise protocol_error("CODEX_TIMEOUT") from None
        except OSError:
            raise protocol_error("CODEX_CONNECTION_LOST") from None

    async def _read_message(self):
        try:
            raw = await self.process.stdout.readline()
        except ValueError:
            raise protocol_error("CODEX_OUTPUT_LIMIT") from None
        if self._limit_exceeded:
            raise protocol_error("CODEX_OUTPUT_LIMIT")
        if not raw:
            raise protocol_error("CODEX_CONNECTION_LOST")
        self._account(len(raw))
        if len(raw) > self.max_line_bytes:
            raise protocol_error("CODEX_OUTPUT_LIMIT")
        try:
            message = json.loads(raw)
        except (ValueError, UnicodeError):
            raise protocol_error("CODEX_PROTOCOL_INVALID") from None
        if not isinstance(message, dict):
            raise protocol_error("CODEX_PROTOCOL_INVALID")
        if "method" in message and "id" in message:
            # Never answer approvals/tool calls on behalf of the user.
            raise protocol_error("CODEX_REQUIRES_USER_INPUT")
        return message

    async def wait_turn_completed(self, thread_id: str, turn_id: str) -> str:
        """Keep draining this session after ACK; only an exact terminal notice proves completion."""
        while True:
            message = self._completed.pop(0) if self._completed else await self._read_message()
            if message.get("method") != "turn/completed":
                continue
            params = message.get("params", {})
            if not isinstance(params, dict) or not isinstance(params.get("turn"), dict):
                raise protocol_error("CODEX_PROTOCOL_INVALID")
            if params.get("threadId") != thread_id or params["turn"].get("id") != turn_id:
                continue
            state = params["turn"].get("status")
            if state not in {"completed", "failed", "interrupted"}:
                raise protocol_error("CODEX_PROTOCOL_INVALID")
            return state.upper()

    async def drain_notifications(self):
        """Drain a resumed owner's bounded stream without inferring any outcome.

        A separately leased complete history observation supplies exact identity
        and persisted terminal evidence. Approvals and tool requests fail closed.
        """
        self._completed.clear()
        while True:
            await self._read_message()
