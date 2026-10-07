# AGENTS.md

This repository contains the local, STDIO-only `hpc-mcp` package. It must remain independent of websites and communicate with compute servers only through SSH/SFTP.

## Installation requests

Only follow installation routing when the human asks to install (for example,
“帮我安装” with this URL). A URL supplied for review or explanation does not
authorize execution. Read [docs/install-with-codex.md](docs/install-with-codex.md)
completely first. Ask for the user's own SSH username and authorized local project
directory. Keep the explicit site IP unless they use another cluster. Passwords
belong only in the local masked GUI, never chat, CLI arguments, environment,
logs, or keyring. Preserve existing configuration. Verify release hashes and
present the installer plan before apply under the human's installation request.
No automatic remote job.

## Development rules

- Use Python 3.11+ and `uv`; keep `uv.lock` synchronized with dependency changes.
- Preserve project-scoped path checks, host trust, command policy, bounded output, and Slurm submission idempotency.
- Never put passwords, key passphrases, credentials, host-specific secrets, or production fixtures in code, schemas, logs, tests, documentation examples, or Git.
- Keep generated files, local databases, virtual environments, staging copies, coverage, and build output untracked.
- Add or update tests for behavior changes. The default local baseline excludes `real_cluster` tests.
- Real-server writes, Slurm submissions, installation over the active tool, and production registration require explicit authorization.

## Baseline checks

```bash
uv sync --extra test --locked
uv run pytest -m "not real_cluster" --cov=hpc_mcp --cov-report=term-missing --cov-fail-under=85
uv build
```

