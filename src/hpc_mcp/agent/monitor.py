"""Polling engine. The Slurm adapter is injected; this module never invokes Codex."""
from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Protocol

from hpc_mcp.agent.store import AgentStore, TERMINAL_STATES
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.slurm.query import SlurmJob


class SlurmAdapter(Protocol):
    async def query_squeue(self, server_alias: str, job_ids: list[str]) -> dict[str, SlurmJob]: ...
    async def query_sacct(self, server_alias: str, job_ids: list[str]) -> dict[str, SlurmJob]: ...


class Monitor:
    def __init__(self, store: AgentStore, slurm: SlurmAdapter, config_version: Callable[[], str],
                 *, jitter: Callable[[], float] = lambda: random.uniform(0, 10),
                 wait: Callable[[float], Awaitable[None]] | None = None,
                 after_poll: Callable[[], Awaitable[None]] | None = None) -> None:
        self.store, self.slurm, self.config_version = store, slurm, config_version
        self.jitter, self.wait = jitter, wait
        self.after_poll = after_poll

    async def poll_once(self, *, now: float | None = None) -> None:
        now = time.time() if now is None else now
        groups = defaultdict(list)
        for watch in self.store.watches(active=True):
            groups[watch.server_alias].append(watch)
        # A missing/unreadable config must not reset an authentication pause.
        version = self.config_version()
        for alias, watches in groups.items():
            if not self.store.server_ready(alias, version, now=now):
                continue
            try:
                queued = await self.slurm.query_squeue(alias, [w.job_id for w in watches])
                missing = []
                for watch in watches:
                    row = queued.get(watch.job_id)
                    self.store.set_state(watch.id, row.state if row else "VERIFYING")
                    if row is None:
                        missing.append(watch)
                accounted = await self.slurm.query_sacct(alias, [w.job_id for w in missing]) if missing else {}
                for watch in missing:
                    row = accounted.get(watch.job_id)
                    state = row.state.split()[0].rstrip("+") if row and row.state else ""
                    if state in TERMINAL_STATES:
                        self.store.record_terminal(watch.id, state, now=now)
                self.store.retry_server(alias)
            except HpcMcpError as error:
                self.store.server_error(alias, error.code, now=now, pause=error.code in {
                    "AUTH_FAILED", "HOST_KEY_CHANGED", "HOST_KEY_VERIFICATION_FAILED"})
            except (OSError, TimeoutError):
                self.store.server_error(alias, "SSH_TRANSPORT_FAILED", now=now)

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                await self.poll_once()
            except HpcMcpError:
                # Local profile errors leave watches pending for correction.
                pass
            if self.after_poll:
                await self.after_poll()
            delay = 45 + max(0, min(10, self.jitter()))
            if self.wait:
                await self.wait(delay)
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
