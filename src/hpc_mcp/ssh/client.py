import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

import asyncssh

from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.policy import CommandPolicy, GeneralCommandPolicy, RemoteCommand
from hpc_mcp.ssh.protocol import CommandResult
from hpc_mcp.ssh.trust import HostTrustStore, TofuHostKeyVerifier

_OUTPUT_READ_BYTES = 64 * 1024


class SshClient:
    def __init__(
        self,
        config: ClusterConfig,
        host_trust: HostTrustStore,
        policy: CommandPolicy | None = None,
    ) -> None:
        self._config = config
        self._host_trust = host_trust
        self._policy = policy or CommandPolicy()

    async def run_general(self, cmd_string: str, directory: str,
                          timeout_ms: int | None) -> CommandResult:
        command = GeneralCommandPolicy(self._config).validate(cmd_string, directory, timeout_ms)
        mapped_error = None
        process = None
        tasks: list[asyncio.Task] = []
        try:
            async with self._connect() as connection:
                async with asyncio.timeout(command.timeout_seconds):
                    process = await connection.create_process(
                        command.render(), encoding=None, stdin=asyncssh.DEVNULL
                    )
                    budget = [self._config.max_output_bytes]
                    tasks = [
                        asyncio.create_task(self._capture_general(process.stdout, budget)),
                        asyncio.create_task(self._capture_general(process.stderr, budget)),
                        asyncio.create_task(process.wait_closed()),
                    ]
                    try:
                        stdout, stderr, _ = await asyncio.gather(*tasks)
                    finally:
                        process.close()
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
        except HpcMcpError:
            raise
        except (asyncssh.Error, OSError, TimeoutError) as error:
            mapped_error = self._mapped_error(error)
        if mapped_error is not None:
            raise mapped_error from None
        return CommandResult(exit_status=process.exit_status, stdout=self._redact(stdout),
                             stderr=self._redact(stderr))

    async def _capture_general(self, stream: Any, budget: list[int]) -> str:
        retained = bytearray()
        while True:
            chunk = await stream.read(min(_OUTPUT_READ_BYTES, self._config.max_output_bytes))
            if not chunk:
                return retained.decode("utf-8", errors="replace")
            budget[0] -= len(chunk)
            if budget[0] <= 0:
                raise HpcMcpError("OUTPUT_LIMIT_EXCEEDED", "SSH output reached the configured byte limit")
            retained.extend(chunk)

    def _redact(self, value: str) -> str:
        for secret in (self._config.password, self._config.private_key, self._config.passphrase):
            if secret is not None and secret.get_secret_value():
                value = value.replace(secret.get_secret_value(), "[REDACTED]")
        return value

    async def run(self, command: RemoteCommand) -> CommandResult:
        validated = self._policy.validate(command)
        rendered = validated.render()
        mapped_error: HpcMcpError | None = None
        try:
            async with self._connect() as connection:
                # A call may shorten, but never extend, the profile's deadline.
                timeout = min(
                    validated.timeout_seconds, self._config.command_timeout_seconds
                )
                async with asyncio.timeout(timeout):
                    process = await connection.create_process(
                        rendered,
                        encoding=None,
                        stdin=asyncssh.DEVNULL,
                    )
                    stdout_result, stderr_result, _ = await asyncio.gather(
                        self._drain_output(process.stdout),
                        self._drain_output(process.stderr),
                        process.wait_closed(),
                    )
        except HpcMcpError:
            raise
        except (asyncssh.Error, OSError, TimeoutError) as error:
            mapped_error = self._mapped_error(error)

        if mapped_error is not None:
            raise mapped_error from None

        stdout, stdout_truncated = stdout_result
        stderr, stderr_truncated = stderr_result
        return CommandResult(
            exit_status=process.exit_status,
            stdout=stdout,
            stderr=stderr,
            truncated=stdout_truncated or stderr_truncated,
        )

    @asynccontextmanager
    async def sftp(self) -> AsyncIterator[Any]:
        # One deadline covers setup and every operation in the caller's SFTP
        # context (including realpath and streamed reads/writes), not just setup.
        deadline = asyncio.timeout(self._config.sftp_timeout_ms / 1000)
        timeout_error: HpcMcpError | None = None
        try:
            async with deadline:
                async with self._sftp_session() as client:
                    yield client
        except TimeoutError as error:
            if not deadline.expired():
                raise
            timeout_error = self._mapped_error(error)
        if timeout_error is not None:
            raise timeout_error from None

    @asynccontextmanager
    async def _sftp_session(self) -> AsyncIterator[Any]:
        mapped_error: HpcMcpError | None = None
        try:
            connection_manager, client = await self._start_sftp()
        except HpcMcpError:
            raise
        except (asyncssh.Error, OSError, TimeoutError) as error:
            mapped_error = self._mapped_error(error)

        if mapped_error is not None:
            raise mapped_error from None

        caller_error: BaseException | None = None
        try:
            yield client
        except BaseException as error:
            caller_error = error
            raise
        finally:
            teardown_error: HpcMcpError | None = None
            try:
                await connection_manager.__aexit__(None, None, None)
            except (asyncssh.Error, OSError, TimeoutError) as error:
                if caller_error is None:
                    teardown_error = self._mapped_error(error)
            if teardown_error is not None:
                raise teardown_error from None

    async def realpath(self, path: str) -> str:
        mapped_error: HpcMcpError | None = None
        async with self.sftp() as client:
            try:
                resolved = await client.realpath(path)
            except HpcMcpError:
                raise
            except (asyncssh.Error, OSError, TimeoutError) as error:
                mapped_error = self._mapped_error(error)

        if mapped_error is not None:
            raise mapped_error from None
        return resolved

    async def _start_sftp(self) -> tuple[Any, Any]:
        connection_manager = self._connect()
        connection = await connection_manager.__aenter__()
        try:
            async with asyncio.timeout(self._config.sftp_timeout_ms / 1000):
                client = await connection.start_sftp_client()
        except BaseException:
            await connection_manager.__aexit__(None, None, None)
            raise
        return connection_manager, client

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[Any]:
        verifier = TofuHostKeyVerifier(
            self._host_trust,
            self._config.alias,
            self._config.host,
            self._config.port,
        )
        password = (
            self._config.password.get_secret_value()
            if self._config.password is not None
            else None
        )
        private_key = (
            self._config.private_key.get_secret_value()
            if self._config.private_key is not None
            else None
        )
        passphrase = (
            self._config.passphrase.get_secret_value()
            if self._config.passphrase is not None
            else None
        )
        preferred_auth = (
            ("publickey", "keyboard-interactive", "password")
            if private_key and password
            else ("publickey",)
            if private_key
            else ("keyboard-interactive", "password")
        )
        async with AsyncExitStack() as stack:
            mapped_error: HpcMcpError | None = None
            try:
                connection = await stack.enter_async_context(asyncssh.connect(
                    self._config.host,
                    port=self._config.port,
                    username=self._config.username,
                    password=password,
                    passphrase=passphrase,
                    known_hosts=b"",
                    connect_timeout=self._config.connect_timeout_seconds,
                    keepalive_interval=self._config.keepalive_interval_ms / 1000,
                    keepalive_count_max=self._config.keepalive_count_max,
                    config=None,
                    client_factory=lambda: verifier,
                    client_keys=[private_key] if private_key else None,
                    agent_path=None,
                    public_key_auth=private_key is not None,
                    host_based_auth=False,
                    gss_kex=False,
                    gss_auth=False,
                    kbdint_auth=password is not None,
                    password_auth=password is not None,
                    preferred_auth=preferred_auth,
                    disable_trivial_auth=True,
                ))
            except (asyncssh.KeyImportError, asyncssh.KeyEncryptionError) as error:
                # Local key loading raises ValueError subclasses, not Error.
                # Normalize setup only, leaving unrelated/caller errors intact.
                mapped_error = self._mapped_error(error)
            if mapped_error is not None:
                # Raise outside the handler so no sensitive context is retained.
                raise mapped_error from None

            # asyncssh enters this block only after authentication succeeds.
            verifier.commit_authenticated()
            yield connection

    async def _drain_output(self, stream: Any) -> tuple[str, bool]:
        retained = bytearray()
        truncated = False

        while True:
            chunk = await stream.read(_OUTPUT_READ_BYTES)
            if not chunk:
                break

            limit = self._config.max_output_bytes
            remaining = limit - len(retained)
            if remaining > 0:
                retained.extend(chunk[:remaining])
            if len(chunk) > remaining:
                truncated = True

        return retained.decode("utf-8", errors="ignore"), truncated

    @staticmethod
    def _mapped_error(error: BaseException) -> HpcMcpError:
        if isinstance(error, (asyncssh.PermissionDenied, asyncssh.KeyImportError,
                              asyncssh.KeyEncryptionError)):
            mapped = HpcMcpError("AUTH_FAILED", "SSH authentication failed")
        elif isinstance(error, asyncssh.HostKeyNotVerifiable):
            mapped = HpcMcpError(
                "HOST_KEY_VERIFICATION_FAILED",
                "SSH host key verification failed",
            )
        elif isinstance(error, TimeoutError):
            mapped = HpcMcpError(
                "SSH_TIMEOUT", "SSH operation timed out", retriable=True
            )
        else:
            mapped = HpcMcpError(
                "SSH_TRANSPORT_FAILED",
                "SSH transport failed",
                retriable=True,
            )
        return mapped
