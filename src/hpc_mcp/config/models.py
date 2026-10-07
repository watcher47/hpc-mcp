from __future__ import annotations

import re
from typing import Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


class ClusterConfig(BaseModel):
    """One local SSH profile with redacted credential rendering."""

    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, validate_assignment=True
    )

    alias: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")
    host: str = Field(min_length=1)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1)
    password: SecretStr | None = Field(default=None, repr=False)
    private_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("privateKey", "private_key"),
        serialization_alias="privateKey",
        repr=False,
    )
    passphrase: SecretStr | None = Field(default=None, repr=False)
    remote_projects_root: str = Field(
        default="~/codex-projects",
        validation_alias=AliasChoices("remoteProjectsRoot", "remote_projects_root"),
        serialization_alias="remoteProjectsRoot",
    )
    host_key_policy: Literal["tofu"] = Field(
        default="tofu",
        validation_alias=AliasChoices("hostKeyPolicy", "host_key_policy"),
        serialization_alias="hostKeyPolicy",
    )
    command_timeout_ms: int = Field(
        default=30_000,
        gt=0,
        le=600_000,
        validation_alias=AliasChoices("commandTimeoutMs", "command_timeout_ms"),
        serialization_alias="commandTimeoutMs",
    )
    connection_timeout_ms: int = Field(
        default=15_000,
        gt=0,
        le=60_000,
        validation_alias=AliasChoices("connectionTimeoutMs", "connection_timeout_ms"),
        serialization_alias="connectionTimeoutMs",
    )
    sftp_timeout_ms: int = Field(
        default=300_000,
        gt=0,
        le=3_600_000,
        validation_alias=AliasChoices("sftpTimeoutMs", "sftp_timeout_ms"),
        serialization_alias="sftpTimeoutMs",
    )
    keepalive_interval_ms: int = Field(
        default=10_000,
        ge=0,
        le=300_000,
        validation_alias=AliasChoices("keepaliveIntervalMs", "keepalive_interval_ms"),
        serialization_alias="keepaliveIntervalMs",
    )
    keepalive_count_max: int = Field(
        default=3,
        ge=1,
        le=100,
        validation_alias=AliasChoices("keepaliveCountMax", "keepalive_count_max"),
        serialization_alias="keepaliveCountMax",
    )
    max_output_bytes: int = Field(
        default=10_485_760,
        gt=0,
        strict=True,
        le=1_073_741_824,
        validation_alias=AliasChoices("maxOutputBytes", "max_output_bytes"),
        serialization_alias="maxOutputBytes",
    )
    whitelist: list[str] = Field(default_factory=list)
    blacklist: list[str] = Field(default_factory=list)
    allowed_local_paths: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("allowedLocalPaths", "allowed_local_paths"),
        serialization_alias="allowedLocalPaths",
    )
    allowed_remote_paths: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("allowedRemotePaths", "allowed_remote_paths"),
        serialization_alias="allowedRemotePaths",
    )
    transport_mode: Literal["exec"] = Field(
        default="exec",
        validation_alias=AliasChoices("transportMode", "transport_mode"),
        serialization_alias="transportMode",
    )

    @model_validator(mode="after")
    def require_authentication(self) -> ClusterConfig:
        if self.password is None and self.private_key is None:
            raise ValueError("an authentication method is required")
        return self

    @field_validator("whitelist", "blacklist")
    @classmethod
    def validate_patterns(cls, patterns: list[str]) -> list[str]:
        for pattern in patterns:
            re.compile(pattern)
        return patterns

    @property
    def connect_timeout_seconds(self) -> float:
        return self.connection_timeout_ms / 1000

    @property
    def command_timeout_seconds(self) -> float:
        return self.command_timeout_ms / 1000


class ProjectFileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    server_alias: str = "default"
    remote_directory: str | None = None
    slurm_script: str | None = None
    log_directory: str = "output"
    artifact_directory: str = "output"
    include: list[str] = Field(default_factory=lambda: ["**/*"])
    exclude: list[str] = Field(
        default_factory=lambda: [".git/**", "**/__pycache__/**", "**/*.pyc"]
    )
