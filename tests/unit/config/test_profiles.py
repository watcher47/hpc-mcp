import json
import os
from pathlib import Path

import pytest
from pydantic import SecretStr

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.config.paths import AppPaths, resolve_servers_path
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError


def _profile(**overrides: object) -> dict[str, object]:
    profile: dict[str, object] = {
        "host": "compute.invalid",
        "port": 2222,
        "username": "fixture-user",
        "password": "fixture-value,{=}",
        "remoteProjectsRoot": "~/codex-projects",
        "hostKeyPolicy": "tofu",
        "commandTimeoutMs": 31_000,
        "connectionTimeoutMs": 16_000,
        "sftpTimeoutMs": 301_000,
        "keepaliveIntervalMs": 11_000,
        "keepaliveCountMax": 4,
        "maxOutputBytes": 2_000_000,
        "whitelist": [r"^squeue(?: .*)?$"],
        "blacklist": [r"^rm(?: .*)?$"],
        "allowedLocalPaths": ["C:/fixture"],
        "allowedRemotePaths": ["/home/fixture-user/codex-projects"],
        "transportMode": "exec",
    }
    profile.update(overrides)
    return profile


def test_loads_object_profiles_and_keeps_credentials_redacted(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"default": _profile()}), encoding="utf-8")

    config = ConfigRepository(path).load_cluster("default")

    assert config.alias == "default"
    assert config.remote_projects_root == "~/codex-projects"
    assert config.connection_timeout_ms == 16_000
    assert isinstance(config.password, SecretStr)
    assert config.password.get_secret_value() == "fixture-value,{=}"
    rendered = repr(config) + str(config.model_dump())
    assert "fixture-value" not in rendered


def test_loads_classfang_array_profiles_by_safe_unique_name(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"
    path.write_text(
        json.dumps(
            [
                {"name": "default", **_profile()},
                {
                    "name": "keyed",
                    **_profile(
                        password=None,
                        privateKey="fixture-key-path",
                        passphrase="fixture-phrase",
                    ),
                },
            ]
        ),
        encoding="utf-8",
    )

    repository = ConfigRepository(path)

    assert repository.load_cluster("default").username == "fixture-user"
    keyed = repository.load_cluster("keyed")
    assert keyed.private_key is not None
    assert keyed.private_key.get_secret_value() == "fixture-key-path"
    assert keyed.passphrase is not None
    assert "fixture-phrase" not in repr(keyed)


def test_object_profile_rejects_conflicting_embedded_name(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"
    path.write_text(
        json.dumps({"default": {"name": "other", **_profile()}}), encoding="utf-8"
    )

    with pytest.raises(HpcMcpError) as error:
        ConfigRepository(path).load_all()

    assert error.value.code == "CONFIG_INVALID"


@pytest.mark.parametrize("payload", [[], [{"name": "bad alias", **_profile()}]])
def test_array_profiles_reject_missing_or_unsafe_aliases(
    tmp_path: Path, payload: object
) -> None:
    path = tmp_path / "servers.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(HpcMcpError) as error:
        ConfigRepository(path).load_all()

    assert error.value.code == "CONFIG_INVALID"


@pytest.mark.parametrize(
    "unsupported",
    [
        {"proxy": "fixture-proxy-value"},
        {"socksProxy": "fixture-proxy-value"},
        {"transportMode": "shell", "shellReadyTimeoutMs": 1_000},
        {"tryKeyboard": True},
        {"commandTemplate": "fixture-template-value"},
    ],
)
def test_unsupported_features_fail_closed_without_leaking_values(
    tmp_path: Path, unsupported: dict[str, object]
) -> None:
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"default": _profile(**unsupported)}), encoding="utf-8")

    with pytest.raises(HpcMcpError) as error:
        ConfigRepository(path).load_cluster("default")

    assert error.value.code == "CONFIG_UNSUPPORTED"
    assert "fixture-proxy-value" not in str(error.value)
    assert "fixture-template-value" not in str(error.value)
    assert error.value.__cause__ is None


def test_profile_requires_password_or_private_key_without_leaking_input(
    tmp_path: Path,
) -> None:
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"default": _profile(password=None)}), encoding="utf-8")

    with pytest.raises(HpcMcpError) as error:
        ConfigRepository(path).load_cluster("default")

    assert error.value.code == "CONFIG_INVALID"
    assert "fixture-user" not in str(error.value)
    assert error.value.__cause__ is None


def test_explicit_config_path_precedes_environment_and_default(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit.json"
    environment = tmp_path / "environment.json"

    assert (
        resolve_servers_path(explicit, {"HPC_MCP_CONFIG": str(environment)})
        == explicit.resolve()
    )
    assert (
        resolve_servers_path(None, {"HPC_MCP_CONFIG": str(environment)})
        == environment.resolve()
    )


def test_config_path_rejects_onedrive_project_location() -> None:
    with pytest.raises(HpcMcpError) as error:
        resolve_servers_path(Path.cwd() / "servers.json", {})

    assert error.value.code == "CONFIG_LOCATION_UNSAFE"


def test_cluster_config_serializes_with_classfang_field_names() -> None:
    config = ClusterConfig.model_validate({"alias": "default", **_profile()})

    payload = config.model_dump(mode="json", by_alias=True)

    assert payload["remoteProjectsRoot"] == "~/codex-projects"
    assert payload["commandTimeoutMs"] == 31_000
    assert "remote_projects_root" not in payload


@pytest.mark.skipif(os.name != "nt", reason="Windows default location contract")
def test_windows_default_config_and_shared_state_locations() -> None:
    assert resolve_servers_path(environ={}) == (
        Path(os.environ["LOCALAPPDATA"]) / "hpc-codex-mcp" / "servers.json"
    ).resolve()
    assert AppPaths.for_user().data_directory == Path.home() / ".hpc-codex-mcp" / "state"


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "0", float("inf"), 1_073_741_825])
def test_profile_output_limit_must_be_positive_bounded_integer(tmp_path, limit) -> None:
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"default": _profile(maxOutputBytes=limit)}))
    with pytest.raises(HpcMcpError) as error:
        ConfigRepository(path).load_all()
    assert error.value.code == "CONFIG_INVALID"
