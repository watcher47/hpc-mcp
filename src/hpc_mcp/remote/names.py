from hpc_mcp.errors import HpcMcpError


def directory_entry_name(name: str | bytes) -> str | None:
    """Decode one SFTP filename; omit sentinels and reject injected paths."""
    if isinstance(name, bytes):
        try:
            name = name.decode("utf-8")
        except UnicodeDecodeError:
            raise HpcMcpError(
                "REMOTE_DECODE_ERROR", "Remote filename is not valid UTF-8"
            ) from None
    if name in {".", ".."}:
        return None
    if not name or any(character in name for character in ("/", "\\", "\x00")):
        raise HpcMcpError(
            "PATH_OUTSIDE_PROJECT", "Remote directory entry is not a single filename"
        ) from None
    return name
