import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.slug import normalized_local_root, path_hash, project_slug


_JOB_ID = re.compile(r"^[0-9]+(?:_[0-9]+)?$")


class ProjectRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    local_root: Path
    server_alias: str
    slug: str
    remote_root: str
    created_at: datetime


class JobRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    project_id: str
    server_alias: str
    job_id: str
    stdout_path: str | None
    stderr_path: str | None
    submitted_at: datetime


class ProjectRegistry:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._initialize()

    def ensure(
        self,
        local_root: Path,
        server_alias: str,
        remote_projects_root: str,
    ) -> ProjectRecord:
        canonical_root = normalized_local_root(local_root)
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT id, local_root, server_alias, slug, remote_root, created_at
                FROM projects
                WHERE local_root = ? AND server_alias = ?
                """,
                (canonical_root, server_alias),
            ).fetchone()
            if existing is not None:
                return self._record(existing)

            slug = project_slug(Path(canonical_root))
            base_root = remote_projects_root.rstrip("/")
            remote_root = f"{base_root}/{slug}" if base_root else f"/{slug}"
            collision = connection.execute(
                """
                SELECT local_root
                FROM projects
                WHERE server_alias = ? AND remote_root = ?
                """,
                (server_alias, remote_root),
            ).fetchone()
            if collision is not None and collision["local_root"] != canonical_root:
                remote_root = f"{remote_root}-{path_hash(Path(canonical_root))}"

            record = ProjectRecord(
                id=str(uuid4()),
                local_root=Path(canonical_root),
                server_alias=server_alias,
                slug=slug,
                remote_root=remote_root,
                created_at=datetime.now(timezone.utc),
            )
            connection.execute(
                """
                INSERT INTO projects (
                    id, local_root, server_alias, slug, remote_root, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    str(record.local_root),
                    record.server_alias,
                    record.slug,
                    record.remote_root,
                    record.created_at.isoformat(),
                ),
            )
            return record

    def get(self, project_id: str) -> ProjectRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, local_root, server_alias, slug, remote_root, created_at
                FROM projects
                WHERE id = ?
                """,
                (project_id,),
            ).fetchone()
        return None if row is None else self._record(row)

    def register_job(
        self,
        project_id: str,
        job_id: str,
        stdout_path: str | None,
        stderr_path: str | None,
    ) -> JobRecord:
        if _JOB_ID.fullmatch(job_id) is None:
            raise HpcMcpError("INVALID_JOB_ID", "Slurm Job ID is invalid")
        with self._connect() as connection:
            project = connection.execute(
                "SELECT server_alias FROM projects WHERE id = ?",
                (project_id,),
            ).fetchone()
            if project is None:
                raise HpcMcpError("PROJECT_NOT_FOUND", "Project is not registered")

            server_alias = str(project["server_alias"])
            existing = connection.execute(
                """
                SELECT id, project_id, server_alias, job_id, stdout_path,
                       stderr_path, submitted_at
                FROM jobs
                WHERE server_alias = ? AND job_id = ?
                """,
                (server_alias, job_id),
            ).fetchone()
            if existing is not None:
                if existing["project_id"] != project_id:
                    raise HpcMcpError(
                        "JOB_ALREADY_REGISTERED",
                        "Job is already registered to another project",
                    )
                return self._job_record(existing)

            record = JobRecord(
                id=str(uuid4()),
                project_id=project_id,
                server_alias=server_alias,
                job_id=job_id,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                submitted_at=datetime.now(timezone.utc),
            )
            connection.execute(
                """
                INSERT INTO jobs (
                    id, project_id, server_alias, job_id, stdout_path,
                    stderr_path, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.project_id,
                    record.server_alias,
                    record.job_id,
                    record.stdout_path,
                    record.stderr_path,
                    record.submitted_at.isoformat(),
                ),
            )
            return record

    def get_job(self, server_alias: str, job_id: str) -> JobRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT id, project_id, server_alias, job_id, stdout_path,
                       stderr_path, submitted_at
                FROM jobs
                WHERE server_alias = ? AND job_id = ?
                """,
                (server_alias, job_id),
            ).fetchone()
        return None if row is None else self._job_record(row)

    def find_job(self, job_id: str) -> JobRecord | None:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, project_id, server_alias, job_id, stdout_path,
                       stderr_path, submitted_at
                FROM jobs
                WHERE job_id = ?
                ORDER BY submitted_at, id
                """,
                (job_id,),
            ).fetchall()
        if len(rows) > 1:
            raise HpcMcpError(
                "JOB_ID_AMBIGUOUS",
                "Job ID is registered on more than one server",
            )
        return None if not rows else self._job_record(rows[0])

    def list_jobs(self) -> list[JobRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, project_id, server_alias, job_id, stdout_path,
                       stderr_path, submitted_at
                FROM jobs
                ORDER BY submitted_at, id
                """
            ).fetchall()
        return [self._job_record(row) for row in rows]

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    local_root TEXT NOT NULL,
                    server_alias TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    remote_root TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(local_root, server_alias),
                    UNIQUE(server_alias, remote_root)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    server_alias TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    submitted_at TEXT NOT NULL,
                    UNIQUE(server_alias, job_id),
                    FOREIGN KEY(project_id) REFERENCES projects(id)
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _record(row: sqlite3.Row) -> ProjectRecord:
        return ProjectRecord.model_validate(dict(row))

    @staticmethod
    def _job_record(row: sqlite3.Row) -> JobRecord:
        return JobRecord.model_validate(dict(row))
