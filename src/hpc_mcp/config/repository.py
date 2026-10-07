from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from pydantic import SecretStr, ValidationError

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.errors import HpcMcpError

_SUPPORTED_FIELDS = {
    "alias",
    "name",
    "host",
    "port",
    "username",
    "password",
    "privateKey",
    "passphrase",
    "remoteProjectsRoot",
    "hostKeyPolicy",
    "commandTimeoutMs",
    "connectionTimeoutMs",
    "sftpTimeoutMs",
    "keepaliveIntervalMs",
    "keepaliveCountMax",
    "maxOutputBytes",
    "whitelist",
    "blacklist",
    "allowedLocalPaths",
    "allowedRemotePaths",
    "transportMode",
}


class ConfigRepository:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load_all(self) -> dict[str, ClusterConfig]:
        payload = self._read_json()
        records: list[tuple[str, Any]]
        if isinstance(payload, dict):
            array_form = False
            if not payload:
                raise self._invalid()
            records = list(payload.items())
        elif isinstance(payload, list):
            array_form = True
            if not payload:
                raise self._invalid()
            records = []
            for value in payload:
                if not isinstance(value, dict) or not isinstance(
                    value.get("name"), str
                ):
                    raise self._invalid()
                records.append((value["name"], value))
        else:
            raise self._invalid()

        clusters: dict[str, ClusterConfig] = {}
        for alias, value in records:
            if alias in clusters or not isinstance(value, dict):
                raise self._invalid(alias if isinstance(alias, str) else None)
            unsupported = set(value) - _SUPPORTED_FIELDS
            if value.get("transportMode", "exec") != "exec":
                unsupported.add("transportMode")
            if unsupported:
                raise HpcMcpError(
                    "CONFIG_UNSUPPORTED",
                    "Server profile contains unsupported features",
                    details={"alias": alias, "fields": sorted(unsupported)},
                )
            if not array_form and "name" in value and value["name"] != alias:
                raise self._invalid(alias)
            normalized = {key: item for key, item in value.items() if key != "name"}
            existing_alias = normalized.pop("alias", alias)
            if existing_alias != alias:
                raise self._invalid(alias)
            normalized["alias"] = alias
            try:
                clusters[alias] = ClusterConfig.model_validate(normalized)
            except (ValidationError, re.error):
                raise self._invalid(alias) from None
        return clusters

    def load_cluster(self, alias: str) -> ClusterConfig:
        try:
            return self.load_all()[alias]
        except KeyError:
            raise HpcMcpError(
                "CONFIG_NOT_FOUND",
                f"Server profile '{alias}' was not found",
                details={"alias": alias},
            ) from None

    def save_cluster(self, config: ClusterConfig) -> None:
        try:
            clusters = self.load_all()
        except HpcMcpError as error:
            if error.code == "CONFIG_READ_FAILED" and not self._path.exists():
                clusters = {}
            else:
                raise
        clusters[config.alias] = config
        self._write_clusters(clusters)

    def delete_cluster(self, alias: str) -> None:
        clusters = self.load_all()
        if alias in clusters:
            del clusters[alias]
            if clusters:
                self._write_clusters(clusters)
            else:
                try:
                    self._path.unlink()
                except OSError:
                    raise HpcMcpError(
                        "CONFIG_WRITE_FAILED", "Could not update server configuration"
                    ) from None

    def _read_json(self) -> Any:
        if not self._path.exists():
            raise HpcMcpError(
                "CONFIG_READ_FAILED",
                "Could not read server configuration",
                details={"path": str(self._path)},
            )
        try:
            return json.loads(self._path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError):
            raise HpcMcpError(
                "CONFIG_READ_FAILED",
                "Could not read server configuration",
                details={"path": str(self._path)},
            ) from None

    def _write_clusters(self, clusters: dict[str, ClusterConfig]) -> None:
        payload = {
            alias: self._config_payload(config) for alias, config in clusters.items()
        }
        temporary_path: Path | None = None
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self._path.parent, delete=False
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                json.dump(payload, temporary_file, indent=2, sort_keys=True)
                temporary_file.write("\n")
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_path, self._path)
        except OSError:
            raise HpcMcpError(
                "CONFIG_WRITE_FAILED",
                "Could not update server configuration",
                details={"path": str(self._path)},
            ) from None
        finally:
            if temporary_path is not None and temporary_path.exists():
                try:
                    temporary_path.unlink()
                except OSError:
                    pass

    @staticmethod
    def _config_payload(config: ClusterConfig) -> dict[str, Any]:
        payload = config.model_dump(mode="python", by_alias=True, exclude={"alias"})
        for key in ("password", "privateKey", "passphrase"):
            value = payload.get(key)
            if isinstance(value, SecretStr):
                payload[key] = value.get_secret_value()
        return payload

    def _invalid(self, alias: str | None = None) -> HpcMcpError:
        details = {"path": str(self._path)}
        if alias is not None:
            details["alias"] = alias
        return HpcMcpError(
            "CONFIG_INVALID", "Server configuration is invalid", details=details
        )
