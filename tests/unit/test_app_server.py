"""Protocol tests use a local fake JSONL child, never a real Codex thread."""
import json
import sys
import asyncio
import subprocess
from pathlib import Path

import pytest

from hpc_mcp.errors import HpcMcpError


@pytest.mark.parametrize("mode,startup_delay", [
    pytest.param(mode, 0, id=mode)
    for mode in ["graceful", "exited", "force", "cancel", "schema", "schema-timeout", "stderr"]
] + [pytest.param("schema", .5, id="schema-delayed-startup")])
def test_child_resources_finalize_before_fresh_loop_closes(mode, startup_delay):
    probe = subprocess.run([sys.executable, str(Path(__file__).parents[1] / "fixtures" / "child_cleanup_probe.py"), mode, str(startup_delay)],
                           capture_output=True, text=True, timeout=15)
    assert probe.returncode == 0, probe.stderr
    result = json.loads(probe.stdout)
    assert result["child_exited"] is True
    assert result["resource_warnings"] == []
    assert result["unraisable_errors"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("terminate_error", [False, True])
@pytest.mark.parametrize("kill_exits", [False, True])
async def test_force_reap_escalates_and_bounds_final_wait(terminate_error, kill_exits):
    from hpc_mcp.agent.app_server import force_close_child

    class Process:
        returncode = None
        calls = []
        stdin = None

        class _transport:
            _loop = None

            @staticmethod
            def close():
                pass

        def terminate(self):
            self.calls.append("terminate")
            if terminate_error:
                raise OSError("untrusted process details")

        def kill(self):
            self.calls.append("kill")
            if kill_exits:
                self.returncode = -9

        async def wait(self):
            if self.returncode is None:
                await asyncio.Event().wait()
            return self.returncode

    process = Process()
    if kill_exits:
        await force_close_child(process, timeout=.01)
        assert process.returncode == -9
    else:
        with pytest.raises(HpcMcpError) as error:
            await asyncio.wait_for(force_close_child(process, timeout=.01), 1)
        assert error.value.code == "CODEX_CHILD_CLEANUP_UNCONFIRMED"
        assert "untrusted" not in str(error.value)
    assert process.calls == ["terminate", "kill"]


def schema_bundle():
    definitions = {
        "InitializeParams": {"type": "object", "required": ["clientInfo"], "properties": {
            "clientInfo": {"type": "object"}, "capabilities": {"type": "object"}}},
        "ThreadReadParams": {"type": "object", "required": ["threadId"], "properties": {
            "threadId": {"type": "string"}, "includeTurns": {"type": "boolean"}}},
        "ThreadResumeParams": {"type": "object", "required": ["threadId"], "properties": {
            "threadId": {"type": "string"}, "cwd": {"type": "string"},
            "excludeTurns": {"type": "boolean"}}},
        "ThreadItemsListParams": {"type": "object", "required": ["threadId"], "properties": {
            "threadId": {"type": "string"}, "cursor": {"type": "string"},
            "limit": {"type": "integer"}, "sortDirection": {"enum": ["asc", "desc"]}}},
        "ThreadTurnsListParams": {"type": "object", "required": ["threadId"], "properties": {
            "threadId": {"type": "string"}, "limit": {"type": "integer", "minimum": 0},
            "sortDirection": {"enum": ["asc", "desc"]},
            "itemsView": {"enum": ["notLoaded", "summary", "full"]}}},
        "TurnStartParams": {"type": "object", "required": ["threadId", "input"], "properties": {
            "threadId": {"type": "string"}, "input": {"type": "array"},
            "clientUserMessageId": {"type": "string"}, "turnTrigger": {"type": "string"}}},
    }
    methods = {"initialize": "InitializeParams", "thread/read": "ThreadReadParams",
               "thread/resume": "ThreadResumeParams", "turn/start": "TurnStartParams",
               "thread/items/list": "ThreadItemsListParams",
               "thread/turns/list": "ThreadTurnsListParams"}
    return {"definitions": definitions, "oneOf": [
        {"type": "object", "properties": {"id": {"type": "integer"},
            "method": {"enum": [method]}, "params": {"$ref": "#/definitions/" + name}},
         "required": ["id", "method", "params"]} for method, name in methods.items()]}


def contract():
    from hpc_mcp.agent.app_server import ProtocolSchema
    return ProtocolSchema(schema_bundle(), {"oneOf": [{"properties": {
        "method": {"enum": ["initialized"]}}, "required": ["method"], "type": "object"}]})


@pytest.mark.asyncio
@pytest.mark.parametrize("rpc_code,expected", [(-32000, -32000), (True, None), (2**40, None),
                                             ("private secret", None)])
async def test_jsonl_rpc_error_retains_only_method_and_bounded_integer(rpc_code, expected):
    from hpc_mcp.agent.app_server import AppServerClient
    script = """import sys,json
for line in sys.stdin:
    req=json.loads(line)
    if 'id' not in req: continue
    error={'code':CODE, 'message':'private secret', 'data':{'secret':'private secret'}}
    print(json.dumps({'id':req['id'], 'error':error}), flush=True)
""".replace("CODE", repr(rpc_code))
    with pytest.raises(HpcMcpError) as caught:
        async with AppServerClient(contract(), command=[sys.executable, "-u", "-c", script]):
            pass
    assert caught.value.code == "CODEX_REQUEST_FAILED"
    assert caught.value.details == {"rpc_method": "initialize", "rpc_code": expected}
    assert "private secret" not in str(caught.value)


def test_schema_requires_explicit_handoff_fields_and_validates_payload():
    from hpc_mcp.agent.app_server import ProtocolSchema
    valid = contract()
    valid.validate("thread/read", {"threadId": "t", "includeTurns": True})
    with pytest.raises(HpcMcpError) as error:
        valid.validate("thread/read", {"threadId": 9})
    assert error.value.code == "CODEX_PROTOCOL_INCOMPATIBLE"
    for field in ("clientUserMessageId", "turnTrigger"):
        raw = schema_bundle()
        del raw["definitions"]["TurnStartParams"]["properties"][field]
        if field == "turnTrigger":
            assert not ProtocolSchema(raw, valid.notifications).supports_trigger
        else:
            with pytest.raises(HpcMcpError):
                ProtocolSchema(raw, valid.notifications)
    raw = schema_bundle()
    raw["oneOf"] = [branch for branch in raw["oneOf"]
                    if branch["properties"]["method"]["enum"] != ["turn/start"]]
    with pytest.raises(HpcMcpError):
        ProtocolSchema(raw, valid.notifications)


def test_schema_rejects_unknown_payload_fields_even_when_generator_is_permissive():
    with pytest.raises(HpcMcpError):
        contract().validate("thread/read", {"threadId": "t", "unknown": "x"})


@pytest.mark.asyncio
async def test_jsonl_handshake_bounded_child_and_notification_interleaving(tmp_path):
    from hpc_mcp.agent.app_server import AppServerClient
    script = """import sys,json
for line in sys.stdin:
    req=json.loads(line)
    if 'id' not in req: continue
    print(json.dumps({'method':'thread/status/changed','params':{}}), flush=True)
    print(json.dumps({'id':req['id'],'result':{'echo':req['method']}}), flush=True)
"""
    async with AppServerClient(contract(), command=[sys.executable, "-u", "-c", script]) as client:
        assert (await client.request("thread/read", {"threadId": "t"}))["echo"] == "thread/read"
        child = client.process
    assert child.returncode is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("behavior,code", [
    ("print('x'*10000,flush=True)", "CODEX_OUTPUT_LIMIT"),
    ("print('not-json',flush=True)", "CODEX_PROTOCOL_INVALID"),
    ("import time; time.sleep(30)", "CODEX_TIMEOUT"),
    ("pass", "CODEX_CONNECTION_LOST"),
    ("import sys; sys.stderr.write('x'*10000);sys.stderr.flush();import time;time.sleep(30)", "CODEX_OUTPUT_LIMIT"),
])
async def test_child_failure_is_bounded_sanitized_and_reaped(behavior, code):
    from hpc_mcp.agent.app_server import AppServerClient
    client = AppServerClient(contract(), command=[sys.executable, "-u", "-c", behavior],
                             timeout=0.3 if code == "CODEX_TIMEOUT" else 5,
                             max_line_bytes=1024, max_output_bytes=2048)
    with pytest.raises(HpcMcpError) as error:
        async with client:
            pass
    assert error.value.code == code
    assert client.process.returncode is not None
    assert "not-json" not in str(error.value)


@pytest.mark.asyncio
async def test_missing_binary_does_not_leak_operating_system_error():
    from hpc_mcp.agent.app_server import AppServerClient
    with pytest.raises(HpcMcpError) as error:
        async with AppServerClient(contract(), command=["missing-hpc-test-executable"]):
            pass
    assert error.value.code == "CODEX_UNAVAILABLE"


@pytest.mark.asyncio
async def test_schema_generation_from_injected_cli(tmp_path):
    from hpc_mcp.agent.app_server import generate_schema
    raw, notices = schema_bundle(), contract().notifications
    script = ("import sys,pathlib,json; "
        "p=pathlib.Path(sys.argv[sys.argv.index('--out')+1]); "
        f"(p/'ClientRequest.json').write_text({json.dumps(json.dumps(raw))}); "
        f"(p/'ClientNotification.json').write_text({json.dumps(json.dumps(notices))})")
    generated = await generate_schema(command=[sys.executable, "-c", script])
    assert generated.supports_trigger


@pytest.mark.asyncio
@pytest.mark.parametrize("script", ["raise SystemExit(1)", "import time;time.sleep(30)",
                                    "print('x'*20000)"])
async def test_schema_generation_failure_never_exposes_child_output(script):
    from hpc_mcp.agent.app_server import generate_schema
    with pytest.raises(HpcMcpError):
        await generate_schema(command=[sys.executable, "-u", "-c", script], timeout=0.2, max_output_bytes=1024)


@pytest.mark.parametrize("raw", [None, [], {}, {"oneOf": [None]}, {"oneOf": [17]}])
def test_invalid_schema_shape_fails_closed(raw):
    from hpc_mcp.agent.app_server import ProtocolSchema
    with pytest.raises(HpcMcpError) as error:
        ProtocolSchema(raw, contract().notifications)
    assert error.value.code == "CODEX_PROTOCOL_INCOMPATIBLE"


@pytest.mark.asyncio
async def test_schema_timeout_reaps_writer_before_temporary_directory_cleanup():
    from hpc_mcp.agent.app_server import generate_schema
    script = ("import sys,pathlib,time; p=pathlib.Path(sys.argv[sys.argv.index('--out')+1]); "
              "f=(p/'open.json').open('w'); f.write('{}'); f.flush(); time.sleep(30)")
    with pytest.raises(HpcMcpError) as error:
        await generate_schema(command=[sys.executable, "-u", "-c", script], timeout=2)
    assert error.value.code == "CODEX_TIMEOUT"


def test_full_request_envelope_is_checked_against_generated_schema():
    from hpc_mcp.agent.app_server import ProtocolSchema
    raw = schema_bundle()
    raw["oneOf"][1]["properties"]["id"] = {"type": "string"}
    incompatible = ProtocolSchema(raw, contract().notifications)
    with pytest.raises(HpcMcpError):
        incompatible.validate("thread/read", {"threadId": "t"})


def test_unresolved_generated_reference_is_a_stable_protocol_error():
    from hpc_mcp.agent.app_server import ProtocolSchema
    raw = schema_bundle()
    raw["definitions"]["ThreadReadParams"]["properties"]["threadId"] = {"$ref": "#/definitions/Missing"}
    incompatible = ProtocolSchema(raw, contract().notifications)
    with pytest.raises(HpcMcpError):
        incompatible.validate("thread/read", {"threadId": "t"})
