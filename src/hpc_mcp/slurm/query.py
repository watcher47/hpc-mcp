from __future__ import annotations

import posixpath
import stat
from collections import defaultdict, deque
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import RemotePathGuard
from hpc_mcp.project.registry import JobRecord, ProjectRegistry
from hpc_mcp.remote.files import (
    READ_LIMIT_BYTES,
    _decode_read_chunk,
    _is_kind,
    _maybe_await,
    _metadata,
    _redact_io_errors,
    _text_name,
)
from hpc_mcp.slurm.parser import parse_squeue_rows
from hpc_mcp.ssh.policy import RemoteCommand
from hpc_mcp.ssh.protocol import SshClientProtocol


ARTIFACT_LIMIT = 2_000
_SQUEUE_FORMAT = "%i|%T|%j|%P|%M|%R"
_SACCT_FORMAT = "JobID%64,State%64,ExitCode,Elapsed,AllocCPUS,ReqMem,NodeList"


@dataclass(frozen=True, slots=True)
class SlurmJob:
    job_id: str
    state: str
    name: str | None = None
    partition: str | None = None
    elapsed: str | None = None
    node_list: str | None = None
    exit_code: str | None = None
    alloc_cpus: str | None = None
    req_mem: str | None = None


@dataclass(frozen=True, slots=True)
class LogChunk:
    job_id: str
    project_id: str
    stream: str
    relative_path: str
    text: str
    offset: int
    next_offset: int | None
    size: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class ArtifactEntry:
    job_id: str
    project_id: str
    relative_path: str
    kind: str
    size: int
    mtime_ns: int


class SlurmQueryService:
    def __init__(
        self,
        registry: ProjectRegistry,
        client: SshClientProtocol | Any,
        *,
        username: str | Any,
        remote_files: Any | None = None,
        artifact_directory: str = "output",
    ) -> None:
        self._registry = registry
        self._client = client
        self._username = username
        # Keep the constructor keyword compatible. Logs and artifacts require
        # bounded SFTP operations, unlike the generic full-hash file reader.
        self._artifact_directory = artifact_directory

    async def get_job(self, job_id: str) -> SlurmJob:
        record = self._registered_job(job_id)
        queued = await self.query_squeue(record.server_alias, [job_id])
        if job_id in queued:
            return queued[job_id]
        accounted = await self.query_sacct(record.server_alias, [job_id])
        return accounted.get(job_id, SlurmJob(job_id=job_id, state="UNKNOWN"))

    async def list_jobs(self) -> list[SlurmJob]:
        records = self._registry.list_jobs()
        by_server: dict[str, list[JobRecord]] = defaultdict(list)
        for record in records:
            by_server[record.server_alias].append(record)

        found: dict[tuple[str, str], SlurmJob] = {}
        for server_alias, server_records in by_server.items():
            job_ids = [record.job_id for record in server_records]
            queued = await self.query_squeue(server_alias, job_ids)
            missing = [job_id for job_id in job_ids if job_id not in queued]
            accounted = (
                await self.query_sacct(server_alias, missing) if missing else {}
            )
            for job_id in job_ids:
                found[(server_alias, job_id)] = queued.get(
                    job_id,
                    accounted.get(job_id, SlurmJob(job_id=job_id, state="UNKNOWN")),
                )
        return [found[(record.server_alias, record.job_id)] for record in records]

    async def query_squeue(
        self, server_alias: str, job_ids: list[str]
    ) -> dict[str, SlurmJob]:
        records = self._owned_records(server_alias, job_ids)
        if not records:
            return {}
        client = self._client_for(server_alias)
        result = await client.run(
            RemoteCommand(
                "squeue",
                (
                    "--noheader",
                    "--user",
                    self._username_for(server_alias),
                    "--jobs",
                    ",".join(record.job_id for record in records),
                    "--format",
                    _SQUEUE_FORMAT,
                ),
                cwd=await self._project_root(client, records[0]),
            )
        )
        # A retired controller record has this exact response. Treat only this
        # complete, empty lookup miss as absent; accounting decides its state.
        if (
            result.exit_status == 1
            and not result.truncated
            and result.stdout == ""
            and result.stderr == "slurm_load_jobs error: Invalid job id specified\n"
        ):
            return {}
        self._check_result(result)
        try:
            rows = parse_squeue_rows(result.stdout)
        except ValueError:
            raise HpcMcpError(
                "SLURM_QUERY_FAILED", "Slurm query returned invalid output"
            ) from None
        requested = {record.job_id for record in records}
        return {
            row.job_id: SlurmJob(
                job_id=row.job_id,
                state=row.state,
                name=row.name,
                partition=row.partition,
                elapsed=row.elapsed,
                node_list=row.node_list,
            )
            for row in rows
            if row.job_id in requested
        }

    async def query_sacct(
        self, server_alias: str, job_ids: list[str]
    ) -> dict[str, SlurmJob]:
        records = self._owned_records(server_alias, job_ids)
        if not records:
            return {}
        client = self._client_for(server_alias)
        result = await client.run(
            RemoteCommand(
                "sacct",
                (
                    "--noheader",
                    "--parsable2",
                    "--user",
                    self._username_for(server_alias),
                    "--jobs",
                    ",".join(record.job_id for record in records),
                    "--format",
                    _SACCT_FORMAT,
                ),
                cwd=await self._project_root(client, records[0]),
            )
        )
        self._check_result(result)
        requested = {record.job_id for record in records}
        jobs: dict[str, SlurmJob] = {}
        try:
            for line in result.stdout.splitlines():
                if not line:
                    continue
                fields = line.split("|")
                if len(fields) != 7:
                    raise ValueError
                job_id, state, exit_code, elapsed, cpus, memory, nodes = fields
                if job_id in requested:
                    jobs[job_id] = SlurmJob(
                        job_id=job_id,
                        state=state,
                        elapsed=elapsed,
                        node_list=nodes,
                        exit_code=exit_code,
                        alloc_cpus=cpus,
                        req_mem=memory,
                    )
        except ValueError:
            raise HpcMcpError(
                "SLURM_QUERY_FAILED", "Slurm query returned invalid output"
            ) from None
        return jobs

    @_redact_io_errors
    async def read_log(
        self, job_id: str, stream: str, offset: int | None = None
    ) -> LogChunk:
        record = self._registered_job(job_id)
        if stream not in {"stdout", "stderr"}:
            raise HpcMcpError("INVALID_LOG_STREAM", "Log stream must be stdout or stderr")
        relative_path = record.stdout_path if stream == "stdout" else record.stderr_path
        if relative_path is None:
            raise HpcMcpError("LOG_NOT_REGISTERED", "Log path is not registered")
        if offset is not None and offset < 0:
            raise HpcMcpError("INVALID_LOG_OFFSET", "Log offset must not be negative")

        client = self._client_for(record.server_alias)
        project = self._registry.get(record.project_id)
        if project is None:
            raise HpcMcpError("PROJECT_NOT_FOUND", "Project is not registered")
        target = await RemotePathGuard.resolve(client, project, relative_path)
        async with client.sftp() as sftp:
            attrs = await _maybe_await(sftp.lstat(target))
            if not _is_kind(attrs, stat.S_ISREG):
                raise HpcMcpError("REMOTE_NOT_FILE", "Remote path is not a regular file")
            size = int(attrs.size or 0)
            start = max(size - READ_LIMIT_BYTES, 0) if offset is None else offset
            handle = await _maybe_await(sftp.open(target, "rb"))
            try:
                await _maybe_await(handle.seek(start))
                data = await _maybe_await(handle.read(READ_LIMIT_BYTES))
            except BaseException:
                try:
                    await _maybe_await(handle.close())
                except Exception:
                    pass
                raise
            else:
                await _maybe_await(handle.close())

        if offset is None and start > 0:
            # A valid UTF-8 sequence has at most three continuation bytes.
            skipped = 0
            while skipped < min(3, len(data)) and data[skipped] & 0xC0 == 0x80:
                skipped += 1
            data = data[skipped:]
            start += skipped
        content, end = _decode_read_chunk(data, start, at_eof=start + len(data) >= size)
        return LogChunk(
            job_id=job_id,
            project_id=record.project_id,
            stream=stream,
            relative_path=relative_path,
            text=content.decode("utf-8"),
            offset=start,
            next_offset=end if end < size else None,
            size=size,
            truncated=start > 0 or end < size,
        )

    @_redact_io_errors
    async def list_artifacts(self, job_id: str) -> list[ArtifactEntry]:
        record = self._registered_job(job_id)
        root = self._safe_relative_directory(self._artifact_directory)
        client = self._client_for(record.server_alias)
        project_root = await self._project_root(client, record)
        pending = deque([root])
        artifacts: list[ArtifactEntry] = []
        examined = 0
        async with client.sftp() as sftp:
            artifact_root, _ = await self._artifact_target(
                client, sftp, project_root, root, project_root
            )
            while pending and examined < ARTIFACT_LIMIT:
                current = pending.popleft()
                target, attrs = await self._artifact_target(
                    client, sftp, project_root, current, artifact_root
                )
                if not _is_kind(attrs, stat.S_ISDIR):
                    raise HpcMcpError("REMOTE_NOT_DIRECTORY", "Remote path is not a directory")
                # Stop the producer before materializing or sorting a large directory.
                async with aclosing(sftp.scandir(target)) as entries:
                    async for item in entries:
                        examined += 1
                        name = _text_name(item.filename)
                        if name not in {".", ".."}:
                            self._safe_relative_path(name)
                            if "/" in name:
                                raise HpcMcpError("PATH_OUTSIDE_PROJECT", "Invalid artifact name")
                            child = posixpath.join(current, name)
                            _, child_attrs = await self._artifact_target(
                                client, sftp, project_root, child, artifact_root
                            )
                            relative = posixpath.relpath(child, root)
                            metadata = _metadata(relative, child_attrs)
                            artifacts.append(ArtifactEntry(
                                job_id=job_id,
                                project_id=record.project_id,
                                relative_path=relative,
                                kind=metadata.kind,
                                size=metadata.size,
                                mtime_ns=metadata.mtime_ns,
                            ))
                            if metadata.kind == "directory":
                                pending.append(child)
                        if examined == ARTIFACT_LIMIT:
                            break
        return sorted(artifacts, key=lambda entry: entry.relative_path)

    @staticmethod
    async def _artifact_target(client, sftp, project_root, relative, boundary):
        """Reject lexical symlinks before canonicalizing; constrain every target."""
        current = project_root
        components = [current]
        for part in PurePosixPath(relative).parts:
            current = posixpath.join(current, part)
            components.append(current)
        for component in components:
            attrs = await _maybe_await(sftp.lstat(component))
            permissions = getattr(attrs, "permissions", None)
            if permissions is None or stat.S_ISLNK(permissions):
                raise HpcMcpError("PATH_OUTSIDE_PROJECT", "Artifact path contains a symbolic link")
        canonical = posixpath.normpath(await client.realpath(current))
        if posixpath.commonpath((boundary, canonical)) != boundary:
            raise HpcMcpError("PATH_OUTSIDE_PROJECT", "Artifact path resolves outside its root")
        return canonical, attrs

    def _registered_job(self, job_id: str) -> JobRecord:
        record = self._registry.find_job(job_id)
        if record is None:
            raise HpcMcpError("JOB_NOT_REGISTERED", "Job is not registered")
        return record

    def _owned_records(
        self, server_alias: str, job_ids: list[str]
    ) -> list[JobRecord]:
        records: list[JobRecord] = []
        for job_id in dict.fromkeys(job_ids):
            record = self._registry.get_job(server_alias, job_id)
            if record is None:
                raise HpcMcpError("JOB_NOT_REGISTERED", "Job is not registered")
            records.append(record)
        return records

    def _client_for(self, server_alias: str) -> SshClientProtocol:
        return self._client(server_alias) if callable(self._client) else self._client

    def _username_for(self, server_alias: str) -> str:
        value = self._username(server_alias) if callable(self._username) else self._username
        if not isinstance(value, str) or not value:
            raise HpcMcpError("CONFIG_INVALID", "Cluster username is invalid")
        return value

    async def _project_root(self, client: SshClientProtocol, record: JobRecord) -> str:
        project = self._registry.get(record.project_id)
        if project is None:
            raise HpcMcpError("PROJECT_NOT_FOUND", "Project is not registered")
        return await RemotePathGuard.resolve(client, project, ".")

    @staticmethod
    def _check_result(result: Any) -> None:
        if result.exit_status != 0 or result.truncated:
            raise HpcMcpError("SLURM_QUERY_FAILED", "Slurm query failed")

    @classmethod
    def _safe_relative_directory(cls, value: str) -> str:
        cls._safe_relative_path(value)
        normalized = posixpath.normpath(value)
        return "." if normalized == "." else normalized.rstrip("/")

    @staticmethod
    def _safe_relative_path(value: str) -> None:
        path = PurePosixPath(value)
        if (
            not value
            or "\x00" in value
            or "\n" in value
            or "\r" in value
            or path.is_absolute()
            or ".." in path.parts
        ):
            raise HpcMcpError(
                "PATH_OUTSIDE_PROJECT", "Path resolves outside the project root"
            )
