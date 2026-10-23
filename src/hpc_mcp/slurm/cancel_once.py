"""Packaged, stdlib-only owner-checked cancellation helper, sent over SSH stdin.

Never execute caller-supplied source, scheduler options, signals, or shell text.
The client records the request intent durably before dispatching this helper.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from uuid import UUID


JOB_ID = re.compile(r"[1-9][0-9]{0,9}(?:_(?:0|[1-9][0-9]{0,9}))?", re.ASCII)
OUTPUT_LIMIT = 65536


def validate_target(job_id: str, request_id: str) -> None:
    if not isinstance(job_id, str) or JOB_ID.fullmatch(job_id) is None:
        raise ValueError("Expected one numeric job ID or one array task ID")
    if str(UUID(request_id)) != request_id:
        raise ValueError("Expected a canonical request UUID")


async def run(program: str, args: list[str]) -> tuple[int, str, str, bool]:
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("SQUEUE_", "SCANCEL_")) and key != "SLURM_CLUSTERS"}
    environment.update(LC_ALL="C", SLURM_BITSTR_LEN="256")
    process = await asyncio.create_subprocess_exec(
        program, *args, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env=environment, limit=8192,
    )

    async def drain(stream):
        data = bytearray()
        truncated = False
        while chunk := await stream.read(4096):
            available = max(0, OUTPUT_LIMIT - len(data))
            data.extend(chunk[:available])
            truncated |= len(chunk) > available
        return data.decode("utf-8", errors="replace"), truncated

    try:
        stdout, stderr, _ = await asyncio.wait_for(
            asyncio.gather(drain(process.stdout), drain(process.stderr), process.wait()), 10,
        )
    except asyncio.TimeoutError:
        if process.returncode is None:
            process.kill()
        await process.wait()
        return -1, "", "timeout", True
    return process.returncode, stdout[0], stderr[0], stdout[1] or stderr[1]


async def cancel(job_id: str, request_id: str) -> dict:
    validate_target(job_id, request_id)
    actor = os.getuid()
    result = dict(request_id=request_id, job_id=job_id, status="rejected",
                  actor_uid=actor, owner_uid=None, previous_states=[], error_code=None)
    try:
        code, stdout, stderr, truncated = await run("squeue", [
            "--noheader", "--array", "--jobs=" + job_id, "--format=%i|%U|%T",
        ])
    except OSError:
        result["error_code"] = "SLURM_QUERY_FAILED"
        return result
    retired = code == 1 and not stdout and stderr.strip() == "slurm_load_jobs error: Invalid job id specified"
    if not truncated and ((code == 0 and not stdout.strip()) or retired):
        result["status"] = "not_active"
        return result
    if code != 0:
        result["error_code"] = "SLURM_QUERY_FAILED"
        return result
    rows = [line.strip().split("|") for line in stdout.splitlines() if line.strip()]
    if truncated or not rows or any(
        len(row) != 3 or JOB_ID.fullmatch(row[0]) is None
        or not (row[0] == job_id or ("_" not in job_id and row[0].startswith(job_id + "_")))
        or not row[1].isascii() or not row[1].isdecimal() or len(row[1]) > 10
        or re.fullmatch(r"[A-Z_]+", row[2]) is None
        for row in rows
    ):
        result["error_code"] = "SLURM_QUERY_INVALID"
        return result
    owners = {int(row[1]) for row in rows}
    if owners != {actor}:
        result["error_code"] = "JOB_NOT_OWNED"
        return result
    result.update(owner_uid=actor, previous_states=sorted({row[2] for row in rows}))
    # The UID filter also protects against ownership changes between query and cancel.
    try:
        code, _, _, truncated = await run("scancel", ["--user=" + str(actor), "--", job_id])
    except OSError:
        code, truncated = -1, False
    if code or truncated:
        result.update(status="uncertain", error_code="CANCELLATION_UNCERTAIN")
    else:
        result["status"] = "cancellation_requested"
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["cancel"])
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--request-id", required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(cancel(args.job_id, args.request_id)), sort_keys=True))


if __name__ == "__main__":
    main()
