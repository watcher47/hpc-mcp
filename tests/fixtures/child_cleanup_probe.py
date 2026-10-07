"""Fresh-loop resource probe; synthetic children only, diagnostics stay filtered."""
import asyncio
import gc
import json
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from hpc_mcp.agent.app_server import AppServerClient, close_child, generate_schema
from hpc_mcp.errors import HpcMcpError


class Schema:
    def validate(self, *args, **kwargs):
        pass


async def run(mode, startup_delay=0):
    if mode in {"schema", "schema-timeout"}:
        children = []
        spawn = asyncio.create_subprocess_exec
        async def capture_child(*args, **kwargs):
            process = await spawn(*args, **kwargs)
            children.append(process)
            return process
        asyncio.create_subprocess_exec = capture_child
        try:
            await generate_schema(command=[sys.executable, "-u", "-c",
                f"import sys,time;time.sleep({startup_delay});sys.stdout.write('x'*400000);sys.stdout.flush();time.sleep(30)"
                if mode == "schema" else "import time;time.sleep(30)"],
                # Output-limit testing must allow fresh interpreter startup;
                # only the independent timeout probe needs a short deadline.
                max_output_bytes=1024, timeout=5 if mode == "schema" else .2)
        except HpcMcpError as error:
            assert error.code == ("CODEX_OUTPUT_LIMIT" if mode == "schema" else "CODEX_TIMEOUT")
        finally:
            asyncio.create_subprocess_exec = spawn
        assert len(children) == 1
        return children[0], children[0].returncode is not None
    child = """import sys,json,time
for line in sys.stdin:
    r=json.loads(line)
    if 'id' in r:
        print(json.dumps({'id':r['id'],'result':{}}),flush=True)
sys.stdout.write('x'*8000)
sys.stdout.flush()
"""
    if mode == "cancel":
        child = child.replace("sys.stdout.write", "time.sleep(.1)\nsys.stdout.write")
    if mode == "stderr":
        child = "import sys,time;sys.stderr.write('x'*400000);sys.stderr.flush();time.sleep(30)"
    client = AppServerClient(Schema(), command=[sys.executable, "-u", "-c", child],
                             max_line_bytes=1024, max_output_bytes=2048 if mode == "stderr" else 16777216)
    if mode == "stderr":
        try:
            await client.__aenter__()
        except HpcMcpError as error:
            assert error.code in {"CODEX_OUTPUT_LIMIT", "CODEX_CONNECTION_LOST"}
        else:
            raise AssertionError("expected output failure")
        return client, client.process.returncode is not None
    await client.__aenter__()
    if mode in {"exited", "force"}:
        client.process.stdin.close()
        while client.process.returncode is None:
            await asyncio.sleep(.01)
    if mode == "force":
        await client.force_reap()
    elif mode == "cancel":
        task = asyncio.create_task(client.__aexit__(None, None, None))
        await asyncio.sleep(.03)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()  # Repeated cancellation must not orphan the cleanup task.
        try:
            await task
        except asyncio.CancelledError:
            pass
    else:
        await client.__aexit__(None, None, None)
        await close_child(client.process)  # Already closed cleanup is idempotent.
    return client, client.process.returncode == 0


unraisable = []
sys.unraisablehook = lambda args: unraisable.append(type(args.exc_value).__name__)
with warnings.catch_warnings(record=True) as captured:
    warnings.simplefilter("always", ResourceWarning)
    retained, exited = asyncio.run(run(sys.argv[1], float(sys.argv[2]) if len(sys.argv) > 2 else 0))
    del retained
    gc.collect()
    resources = [type(w.message).__name__ for w in captured if issubclass(w.category, ResourceWarning)]
print(json.dumps({"child_exited": exited, "resource_warnings": resources, "unraisable_errors": unraisable}))
