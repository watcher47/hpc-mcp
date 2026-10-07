"""A fake subprocess used only by tests; it never imports or calls Codex."""
import json
import sys
import time
from pathlib import Path

state_path = Path(sys.argv[1])
state = json.loads(state_path.read_text())
initialized = False


def save():
    state_path.write_text(json.dumps(state))


def history_entries():
    """Generate large fake history without persisting megabytes per request."""
    entries = [{"turnId": "prior", "item": {"type": "agentMessage", "text": "x" * 1100}}
               for _ in range(state.get("history_item_count", 0))]
    entries.extend({"turnId": turn["id"], "item": item}
                   for turn in state.get("turns", []) for item in turn["items"])
    return entries


for line in sys.stdin:
    request = json.loads(line)
    method = request["method"]
    state.setdefault("calls", []).append(request)
    save()
    if method == "initialized":
        initialized = True
        continue
    if method == "initialize":
        result = {"userAgent": "fake-app-server"}
    else:
        assert initialized
        params = request["params"]
        assert params["threadId"] == "original-thread"
        if method == "thread/read":
            turns = state.get("turns", [])
            if params["includeTurns"] and state.get("history_item_count"):
                turns = [{"items": [entry["item"] for entry in history_entries()]}]
            result = {"thread": {"id": "original-thread", "sessionId": "original-session",
                "historyMode": state.get("history_mode", "legacy"),
                "status": {"type": state.get("status", "idle")},
                "turns": turns if params["includeTurns"] else []}}
        elif method == "thread/items/list":
            offset = int(params.get("cursor", "0"))
            entries = history_entries()
            end = min(offset + params["limit"], len(entries))
            result = {"data": entries[offset:end],
                "nextCursor": str(end) if end < len(entries) else None}
        elif method == "thread/turns/list":
            assert params == {"threadId": "original-thread", "limit": 1,
                              "sortDirection": "desc", "itemsView": "notLoaded"}
            # Synthetic persisted observation is independent of runtime status.
            result = {"data": [{"id": state.get("latest_id", "prior-completed"),
                "status": state.get("latest_status", "completed"),
                "items": [], "itemsView": "notLoaded"}], "nextCursor": None}
        elif method == "thread/resume":
            state["status"] = "idle"
            result = {"thread": {"id": "original-thread", "sessionId": "original-session",
                "historyMode": state.get("history_mode", "legacy"), "status": {"type": "idle"},
                "turns": [] if params.get("excludeTurns") else state.get("turns", [])}}
        elif method == "turn/start":
            state.setdefault("turns", []).append({"id": "accepted-turn", "items": [{
                "id": "message", "type": "userMessage", "clientId": params["clientUserMessageId"],
                "content": params["input"]}]})
            save()
            if state.get("lose_response"):
                sys.exit(0)
            result = {"turn": {"id": "accepted-turn", "status": "inProgress", "items": []}}
        else:
            raise AssertionError(method)
    save()
    print(json.dumps({"method": "test/notification", "params": {}}), flush=True)
    print(json.dumps({"id": request["id"], "result": result}), flush=True)
    if method == "turn/start":
        time.sleep(state.get("work_seconds", 0))
        if state.get("crash_after_ack"):
            sys.exit(1)
        state["artifact"] = "analysis-finished"
        state["turns"][-1]["status"] = "completed"
        save()
        print(json.dumps({"method": "turn/completed", "params": {
            "threadId": "original-thread", "turn": {"id": "accepted-turn", "status": "completed"}}}), flush=True)
state["clean_exits"] = state.get("clean_exits", 0) + 1
save()
