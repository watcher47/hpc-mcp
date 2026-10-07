"""Synthetic bounded native queue interface. No Codex, SSH, or model calls."""
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

path = Path(sys.argv[1])


@contextmanager
def state_lock():
    # The resumed owner and passive readers are separate processes. Serialize
    # complete read/modify/write transactions, not just individual writes.
    lock = path.with_suffix('.lock')
    deadline = time.monotonic() + 3
    while True:
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError('Synthetic queue state lock timed out')
            time.sleep(.005)
    try:
        yield
    finally:
        os.close(fd)
        lock.unlink()


def load():
    return json.loads(path.read_text())


def save(state):
    path.write_text(json.dumps(state))


def respond(request, result):
    print(json.dumps({'id': request['id'], 'result': result}), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    method, params = request['method'], request.get('params', {})
    with state_lock():
        state = load()
        state.setdefault('calls', []).append(request)
        save(state)
        if method == 'initialized':
            continue
        if method == 'initialize':
            respond(request, {'userAgent': 'synthetic-queue'})
            continue
        if method == 'thread/read':
            if state.get('bad') == 'oversized':
                respond(request, {'private transcript': 'x' * 4194304})
                continue
            if state.get('bad') == 'malformed':
                print('private transcript invalid JSON', flush=True)
                continue
            result = {'thread': {'id': 'original-thread', 'sessionId': 'original-session',
                'status': {'type': state.get('status', 'active')}, 'historyMode': 'paginated', 'turns': []}}
        elif method == 'thread/queue/list':
            result = {'data': state.get('queue', []), 'nextCursor': None}
        elif method == 'thread/queue/add':
            q = {'id': 'native-queue', 'input': params['input'],
                'clientUserMessageId': params['clientUserMessageId']}
            state['queue'] = [q]
            save(state)
            result = {'queuedSubmission': q}
        elif method == 'thread/items/list':
            count = state.get('history_count', 0)
            offset = int(params.get('cursor', '0'))
            total = count + len(state.get('turns', []))
            end = min(offset + params['limit'], total)
            entries = []
            for i in range(offset, end):
                if i < count:
                    entries.append({'turnId': 'prior', 'item': {'id': f'history-{i}',
                        'type': 'agentMessage', 'text': 'x' * 1900}})
                else:
                    t = state['turns'][i-count]
                    entries.append({'turnId': t['id'], 'item': t['items'][0]})
            result = {'data': entries, 'nextCursor': str(end) if end < total else None}
        elif method == 'thread/turns/list':
            result = {'data': [dict(t, items=[], itemsView='notLoaded') for t in state.get('turns', [])],
                'nextCursor': None}
        elif method == 'thread/resume':
            assert params == {'threadId': 'original-thread', 'excludeTurns': True}
            result = {'thread': {'id': 'original-thread', 'sessionId': 'original-session',
                'status': {'type': 'idle'}, 'turns': []}}
        else:
            raise AssertionError(method)
    respond(request, result)
    if method == 'thread/resume':
        # Owner execution remains concurrent with passive observation.
        time.sleep(state.get('delay', .1))
        with state_lock():
            state = load()
            q = state['queue'].pop(0)
            state['turns'] = [{'id': 'queued-turn', 'status': 'completed', 'completedAt': 180,
                'items': [{'id': 'native-input', 'type': 'userMessage',
                    'clientId': q['clientUserMessageId'], 'content': q['input']}]}]
            save(state)
        print(json.dumps({'method': 'turn/completed', 'params': {'threadId': 'original-thread',
            'turn': {'id': 'queued-turn', 'status': 'completed', 'completedAt': 180}}}), flush=True)
