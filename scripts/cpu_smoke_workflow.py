"""Reusable CPU acceptance through the canonical installed MCP STDIO service.

No direct SSH client, credential arguments, permission changes or generic job
execution. Keep the same record directory for recovery; submit reuses its UUID.
Run with the installed release Python recorded in installation.json.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "cpu_smoke"
INPUTS = ("smoke.py", "slurm/cpu_smoke.sbatch", "logs/.gitkeep")
ACTIVE = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "UNKNOWN"}


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_or_create_run(path, intent):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump({"request_id": str(uuid4()), "created_at": now(), "intent": intent}, stream)
    except FileExistsError:
        pass
    result = json.loads(path.read_text(encoding="utf-8"))
    # Runtime upgrades do not change submission intent. Preserve the historical
    # command in old records, while still comparing every workload/target field.
    workload = lambda value: {key: item for key, item in value.items() if key != "command"}
    if workload(result["intent"]) != workload(intent):
        raise ValueError("Submission intent changed; preserve this record and reconcile before a new run")
    return result


def unpack(result):
    if result.data is not None:
        return result.data
    blocks = [block.text for block in result.content if getattr(block, "type", None) == "text"]
    if len(blocks) != 1:
        raise ValueError("MCP response has no unambiguous JSON payload")
    return json.loads(blocks[0])


def require_success(job, job_id):
    if job.get("job_id") != job_id or job.get("state") != "COMPLETED" or job.get("exit_code") != "0:0":
        raise ValueError("Exact main-job accounting must be COMPLETED with exit code 0:0")


def verify_transfer(path, receipt):
    if path.stat().st_size != receipt["size"] or sha256(path) != receipt["sha256"]:
        raise ValueError("Transferred file size/hash does not match MCP receipt")


class Workflow:
    def __init__(self, options, client, command):
        self.o, self.client = options, client
        self.record = options.record.resolve()
        self.record.mkdir(parents=True, exist_ok=True)
        intent = {"server": options.server, "host": options.expected_host,
                  "hostname": options.expected_hostname, "user": options.expected_user,
                  "partition": options.partition, "local_root": str(FIXTURE),
                  "command": command, "inputs": {p: sha256(FIXTURE / p) for p in INPUTS}}
        self.run = load_or_create_run(self.record / "run.json", intent)
        self.run["runtime_command"] = command

    def checkpoint(self):
        save(self.record / "run.json", self.run)

    async def call(self, name, **arguments):
        value = unpack(await self.client.call_tool(name, arguments))
        with (self.record / "calls.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"utc": now(), "tool": name, "arguments": arguments,
                                     "result": value}, ensure_ascii=False) + "\n")
        if isinstance(value, dict) and value.get("ok") is False:
            error = value["error"]
            raise RuntimeError(f"{name}: {error['code']}: {error['message']}")
        print(json.dumps({"utc": now(), "tool": name, "ok": True}), flush=True)
        return value

    async def ssh_read(self, command):
        result = await self.call("ssh_execute", cmd_string=command, server_alias=self.o.server)
        if result["exit_status"] != 0 or result["truncated"]:
            raise ValueError("Read-only server probe failed or was truncated")
        return result["stdout"].strip()

    async def probe(self):
        tools = await self.client.list_tools()
        save(self.record / "tool-schema.json", [
            {"name": t.name, "input_schema": t.input_schema} for t in tools])
        servers = await self.call("server_list")
        matches = [s for s in servers if s["alias"] == self.o.server]
        if len(matches) != 1 or (matches[0]["host"], matches[0]["username"]) != (self.o.expected_host, self.o.expected_user):
            raise ValueError("Configured server identity differs from the approved target")
        connection = await self.call("server_test_connection", server_alias=self.o.server)
        if not connection["ready"]:
            raise ValueError("Existing SSH/Slurm configuration is not ready")
        user, host = await self.ssh_read("whoami"), await self.ssh_read("hostname")
        if (user, host) != (self.o.expected_user, self.o.expected_hostname):
            raise ValueError("Actual server identity differs from the approved target")
        partitions = await self.ssh_read("sinfo --noheader --long")
        if not any(row.split()[0].rstrip("*") == self.o.partition and row.split()[1] == "up"
                   for row in partitions.splitlines()):
            raise ValueError("Requested partition is not available")
        nodes = await self.ssh_read("sinfo --noheader --Node --long")
        jobs = await self.call("slurm_list_jobs")
        self.run["probe"] = {"utc": now(), "server": matches[0], "connection": connection,
                             "whoami": user, "hostname": host, "partitions": partitions,
                             "nodes": nodes, "registered_jobs": jobs}
        self.checkpoint()

    async def prepare(self):
        await self.probe()
        project = await self.call("project_ensure", local_root=str(FIXTURE), server_alias=self.o.server)
        if Path(project["local_root"]).resolve() != FIXTURE or project["server_alias"] != self.o.server:
            raise ValueError("Unexpected project mapping")
        self.run["project"] = project
        self.checkpoint()
        status = await self.call("project_status", project_id=project["id"])
        if status != project:
            raise ValueError("Project mapping changed")
        plan = await self.call("project_list_remote_changes", project_id=project["id"])
        if plan["conflicts"]:
            raise ValueError("Remote sync conflict; explicit reconciliation required")
        if set(plan["local_manifest"]["files"]) != set(INPUTS):
            raise ValueError("Fixture upload inventory is not exactly the three approved inputs")
        synced = await self.call("project_sync", project_id=project["id"])
        if synced["failed"] or synced["conflicts"] or not synced["manifest_updated"]:
            raise ValueError("Project sync did not complete cleanly")
        for path in INPUTS:
            remote = await self.call("remote_read", project_id=project["id"], relative_path=path)
            if remote["sha256"] != self.run["intent"]["inputs"][path] or remote["truncated"]:
                raise ValueError("Uploaded input content differs")
        preview = await self.call("slurm_preview", project_id=project["id"], script_path=INPUTS[1])
        resources = preview["resources"]
        if (resources["partition"] != self.o.partition or resources["cpus_per_task"] != 1
                or resources["ntasks"] != 1 or resources["nodes"] != 1
                or resources["memory"] != "128M" or resources["time"] != "00:02:00"
                or resources["gpus"] not in (None, 0) or preview["warnings"]):
            raise ValueError("Preview differs from the approved minimal CPU resources")
        self.run["preview"] = preview
        self.run["prepared_at"] = now()
        self.checkpoint()

    async def submit(self):
        if "receipt" in self.run:
            print("Existing receipt retained; no submission repeated", flush=True)
            return
        await self.prepare()
        self.run["submission_attempted_at"] = now()
        self.checkpoint()  # Persist UUID and intent before the side effect.
        self.run["receipt"] = await self.call("slurm_submit", project_id=self.run["project"]["id"],
            script_path=INPUTS[1], request_id=self.run["request_id"])
        self.checkpoint()

    async def collect(self):
        receipt, project = self.run["receipt"], self.run["project"]
        job_id, project_id = receipt["job_id"], project["id"]
        status = await self.call("project_status", project_id=project_id)
        if status != project:
            raise ValueError("Recorded project mapping changed")
        deadline = time.monotonic() + self.o.wait_seconds
        while True:
            job = await self.call("slurm_get_job", job_id=job_id)
            self.run["accounting"] = job
            self.checkpoint()
            if job["state"] not in ACTIVE or time.monotonic() >= deadline:
                break
            await asyncio.sleep(min(10, max(0, deadline - time.monotonic())))
        if job["state"] in ACTIVE:
            raise RuntimeError("Job still active/accounting pending; collect again using the same record")
        for stream in ("stdout", "stderr"):
            chunk = await self.call("slurm_read_log", job_id=job_id, stream=stream, offset=0)
            save(self.record / (stream + ".json"), chunk)
        require_success(job, job_id)
        artifacts = await self.call("slurm_list_artifacts", job_id=job_id)
        inventory = await self.call("remote_list", project_id=project_id, relative_path=f"output/{job_id}")
        if [(item["relative_path"], item["kind"]) for item in inventory] != [(f"output/{job_id}/result.json", "file")]:
            raise ValueError("Smoke result inventory differs from the expected single artifact")
        if not any(item["relative_path"] == f"{job_id}/result.json" and item["kind"] == "file"
                   for item in artifacts):
            raise ValueError("Job artifact listing does not include the expected result")
        self.run["artifacts"], self.run["inventory"] = artifacts, inventory
        transfers = self.run.setdefault("transfers", {})
        paths = [receipt["stdout_path"], receipt["stderr_path"], f"output/{job_id}/result.json"]
        for remote_path in paths:
            local_relative = f"collected/{self.run['request_id']}/{remote_path}"
            target = FIXTURE / local_relative
            if target.exists():
                if remote_path not in transfers:
                    raise ValueError("Unrecorded local download exists; preserve it and reconcile")
                verify_transfer(target, transfers[remote_path])
            else:
                transfer = await self.call("remote_download", project_id=project_id,
                    remote_relative_path=remote_path, local_relative_path=local_relative)
                transfers[remote_path] = transfer
                self.checkpoint()
                verify_transfer(target, transfer)
        result = json.loads((FIXTURE / transfers[paths[2]]["local_relative_path"]).read_text())
        expected = {"job_id": job_id, "hostname": self.o.expected_hostname, "user": self.o.expected_user,
                    "partition": self.o.partition, "cpus_per_task": 1, "ntasks": 1,
                    "gpu_requested": False, "sum_squares_1_to_100": 338350}
        if any(result.get(k) != v for k, v in expected.items()):
            raise ValueError("Payload result differs from expected identity/resources/calculation")
        for stream in ("stdout", "stderr"):
            chunk = json.loads((self.record / (stream + ".json")).read_text())
            if chunk["truncated"]:
                raise ValueError("Smoke log was unexpectedly truncated")
            if stream == "stderr" and chunk["text"]:
                raise ValueError("Smoke job stderr is not empty")
            if stream == "stdout" and json.loads(chunk["text"]) != result:
                raise ValueError("Stdout and downloaded result differ")
        self.run["result"], self.run["verified_at"] = result, now()
        self.checkpoint()
        print(json.dumps({"verified": True, "job_id": job_id, "state": job["state"],
                          "exit_code": job["exit_code"], "record": str(self.record)}, indent=2), flush=True)

    async def watch_status(self):
        self.run["watch"] = await self.call("slurm_watch_job", job_id=self.run["receipt"]["job_id"],
                                            server_alias=self.o.server)
        self.checkpoint()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("probe", "prepare", "submit", "collect", "watch-status"))
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--expected-host", required=True)
    parser.add_argument("--expected-hostname", required=True)
    parser.add_argument("--expected-user", required=True)
    parser.add_argument("--partition", required=True)
    parser.add_argument("--wait-seconds", type=int, default=0)
    options = parser.parse_args()
    metadata = Path.home() / ".hpc-codex-mcp/state/installation.json"
    command = json.loads(metadata.read_text(encoding="utf-8"))["managed_command"]
    if len(command) != 5 or command[1:4] != ["-m", "hpc_mcp.server", "--config-file"]:
        raise ValueError("Unrecognized installed MCP command; inspect installation metadata")
    from fastmcp import Client
    from fastmcp.client.transports import StdioTransport
    async with Client(StdioTransport(command=command[0], args=command[1:], cwd=str(ROOT))) as client:
        workflow = Workflow(options, client, command)
        await getattr(workflow, options.action.replace("-", "_"))()


if __name__ == "__main__":
    asyncio.run(main())
