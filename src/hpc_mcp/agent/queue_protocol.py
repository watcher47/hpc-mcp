"""Versioned payload and complete, exact native queue/history evidence."""
from dataclasses import dataclass
from hashlib import sha256
import json
import math
from pathlib import Path
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError

from .app_server import protocol_error
from .delivery import completion_message, _item_has_marker
from hpc_mcp.errors import HpcMcpError


def message_digest(message):
    return sha256(message.encode('utf-8')).hexdigest()


def context_digest(watch, event):
    return message_digest(json.dumps({'watch':{key:value for key,value in vars(watch).items()
        if key not in {'last_state','active'}}, 'event':{
        'id':event.id, 'watch_id':event.watch_id, 'terminal_state':event.terminal_state}},
        sort_keys=True, ensure_ascii=True))


def render_queue_message(watch, event, version=2):
    if version == 1:
        return completion_message(watch, event)
    if (version != 2 or not isinstance(watch.cwd, str) or len(watch.cwd) > 4096
            or any(ord(c) < 32 or ord(c) == 127 for c in watch.cwd)
            or not Path(watch.cwd).is_absolute() or not Path(watch.cwd).is_dir()):
        raise protocol_error('CODEX_EVENT_CONTEXT_INVALID')
    return (completion_message(watch, event) + '\nRegistered project working directory (JSON data):\n'
        + json.dumps({'project_cwd':watch.cwd}, ensure_ascii=True)
        + '\nWork in this registered project directory and read its applicable AGENTS.md before '
        'reconciling this event. Verify retained collected artifacts when a fresh collection already '
        'exists; do not overwrite them. If this directory is inaccessible, report that limitation. '
        'For failure or nonzero exit collect diagnostic logs only; do not resubmit or modify code or resources.')


def reconstruct_message(watch, event, receipt):
    message = render_queue_message(watch, event, receipt.message_version)
    if (receipt.format_version != 1 or message_digest(message) != receipt.message_sha256
            or context_digest(watch, event) != receipt.context_sha256):
        raise protocol_error('CODEX_QUEUE_IDENTITY_INVALID')
    return message


def require_queue_schema(schema, *, resume_enabled=True):
    required = {'initialize':{'clientInfo','capabilities'}, 'thread/read':{'threadId','includeTurns'},
        'thread/queue/add':{'threadId','clientUserMessageId','input'},
        'thread/queue/list':{'threadId','cursor','limit'},
        'thread/items/list':{'threadId','cursor','limit','sortDirection'},
        'thread/turns/list':{'threadId','cursor','limit','sortDirection','itemsView'}}
    if resume_enabled:
        required['thread/resume'] = {'threadId','excludeTurns'}
    try:
        for method, fields in required.items():
            if not fields <= schema.methods[method].get('properties', {}).keys():
                raise ValueError()
            Draft7Validator.check_schema(schema.methods[method])
        # Validate actual minimal request shapes, independently of direct-start contracts.
        schema.validate('thread/read', {'threadId':'schema-check', 'includeTurns':True})
        schema.validate('thread/queue/add', {'threadId':'schema-check','clientUserMessageId':'schema-check',
            'input':[{'type':'text','text':'schema-check','text_elements':[]}]})
        schema.validate('thread/queue/list', {'threadId':'schema-check','limit':20})
        schema.validate('thread/items/list', {'threadId':'schema-check','limit':20,'sortDirection':'desc'})
        schema.validate('thread/turns/list', {'threadId':'schema-check','limit':20,
            'sortDirection':'desc','itemsView':'notLoaded'})
        for method in ('thread/queue/list', 'thread/items/list', 'thread/turns/list'):
            params = {'threadId':'schema-check', 'limit':20, 'cursor':'schema-cursor'}
            if method != 'thread/queue/list':
                params['sortDirection'] = 'desc'
            if method == 'thread/turns/list':
                params['itemsView'] = 'notLoaded'
            schema.validate(method, params)
        if resume_enabled:
            schema.validate('thread/resume', {'threadId':'schema-check','excludeTurns':True})
    except (KeyError, TypeError, ValueError, SchemaError):
        raise protocol_error() from None


def terminal_outcome(turn):
    if not isinstance(turn, dict) or not _id(turn.get('id')) or not isinstance(turn.get('status'),str):
        return None
    outcome = {'completed':'COMPLETED','failed':'FAILED','interrupted':'INTERRUPTED'}.get(turn.get('status'))
    end = turn.get('completedAt')
    if outcome is None or not _time(end):
        return None
    start = turn.get('startedAt')
    if start is not None and (not _time(start) or start > end):
        return None
    duration = turn.get('durationMs')
    if duration is not None and not _time(duration):
        return None
    return outcome


def _time(value):
    return type(value) in (int,float) and math.isfinite(value) and 0 <= value <= 2**53


def _id(value):
    return isinstance(value, str) and 0 < len(value) <= 256 and not any(ord(c)<32 for c in value)


@dataclass(frozen=True)
class QueueEvidence:
    exact_count: int = 0
    queue_id: str | None = None
    turn_id: str | None = None
    item_id: str | None = None
    outcome: str | None = None
    complete: bool = True
    identity_kind: str | None = None


async def _pages(client, method, params, schema, max_pages):
    if type(max_pages) is not int or not 0 < max_pages <= 500:
        raise ValueError('Invalid page bound')
    seen = set()
    for _ in range(max_pages):
        if schema is not None:
            schema.validate(method, params)
        page = await client.request(method, params)
        if (not isinstance(page, dict) or not isinstance(page.get('data'), list)
                or len(page['data']) > params.get('limit', 20) or 'nextCursor' not in page):
            raise protocol_error('CODEX_HISTORY_INCOMPLETE')
        yield page['data']
        cursor = page['nextCursor']
        if cursor is None:
            return
        if not _id(cursor) or cursor in seen:
            raise protocol_error('CODEX_HISTORY_INCOMPLETE')
        seen.add(cursor)
        params = dict(params, cursor=cursor)
    raise protocol_error('CODEX_HISTORY_INCOMPLETE')


def _exact(item, event_id, message):
    exact = _item_has_marker(item, event_id, message)
    if item.get('type') == 'userMessage' and item.get('clientId') == event_id and not exact:
        raise protocol_error('CODEX_QUEUE_CONFLICT')
    return exact


def validate_queue_ack(result, event_id, expected_message):
    entry = result.get('queuedSubmission') if isinstance(result,dict) else None
    if (not isinstance(entry,dict) or not _id(entry.get('id'))
            or entry.get('clientUserMessageId') != event_id
            or not _exact({'type':'userMessage','clientId':entry.get('clientUserMessageId'),
                'content':entry.get('input')},event_id,expected_message)):
        raise protocol_error('CODEX_QUEUE_IDENTITY_INVALID')
    return entry['id']


async def read_queue_evidence(client, thread_id, event_id, expected_message, *, schema=None, max_pages=500,
                              acknowledged_queue_id=None):
    found, seen = [], set()
    async for entries in _pages(client,'thread/queue/list',{'threadId':thread_id,'limit':20},schema,max_pages):
        for entry in entries:
            if not isinstance(entry, dict) or not _id(entry.get('id')):
                raise protocol_error('CODEX_HISTORY_INCOMPLETE')
            if entry['id'] in seen:
                raise protocol_error('CODEX_HISTORY_INCOMPLETE')
            seen.add(entry['id'])
            proxy = {'type':'userMessage','content':entry.get('input'),'clientId':entry.get('clientUserMessageId')}
            exact = _exact(proxy,event_id,expected_message)
            if acknowledged_queue_id and ((entry['id'] == acknowledged_queue_id) != exact):
                raise protocol_error('CODEX_QUEUE_CONFLICT')
            if exact:
                found.append(entry['id'])
    if len(found) > 1:
        raise protocol_error('CODEX_QUEUE_CONFLICT')
    return QueueEvidence(len(found),queue_id=found[0] if found else None)


async def read_identified_history(client, thread, event_id, expected_message, *, schema=None, max_pages=500,
                                  acknowledged_null_client=False):
    found, seen = [], set()
    def inspect(item, turn_id, turn=None):
        if not isinstance(item, dict) or not _id(item.get('id')) or not _id(turn_id):
            raise protocol_error('CODEX_HISTORY_INCOMPLETE')
        if item['id'] in seen:
            raise protocol_error('CODEX_HISTORY_INCOMPLETE')
        seen.add(item['id'])
        kind = 'NATIVE_CLIENT_ID'
        try:
            exact = _exact(item,event_id,expected_message)
        except HpcMcpError as error:
            if error.code != 'CODEX_HISTORY_IDENTITY_UNAVAILABLE':
                raise
            if item.get('clientId') is not None:
                raise protocol_error('CODEX_QUEUE_CONFLICT') from None
            if not acknowledged_null_client:
                raise
            # _exact has validated the complete single text part. The fallback
            # additionally requires byte-exact text: no normalization or marker
            # substring can stand in for the persisted message digest.
            if (item.get('clientUserMessageId') not in (None, event_id)
                    or item['content'][0]['text'] != expected_message):
                raise protocol_error('CODEX_QUEUE_CONFLICT') from None
            exact, kind = True, 'ACK_MESSAGE_NATIVE_ITEM'
        if exact:
            found.append((turn_id,item['id'],terminal_outcome(turn),kind))
    if thread.get('historyMode','legacy') == 'legacy':
        if not isinstance(thread.get('turns'),list):
            raise protocol_error('CODEX_HISTORY_INCOMPLETE')
        turns = set()
        for turn in thread['turns']:
            if (not isinstance(turn,dict) or not _id(turn.get('id')) or turn['id'] in turns
                    or not isinstance(turn.get('items'),list) or turn.get('itemsView','full') != 'full'):
                raise protocol_error('CODEX_HISTORY_INCOMPLETE')
            turns.add(turn['id'])
            for item in turn['items']:
                inspect(item,turn['id'],turn)
    elif thread.get('historyMode') == 'paginated':
        async for entries in _pages(client,'thread/items/list',
                {'threadId':thread['id'],'limit':20,'sortDirection':'desc'},schema,max_pages):
            for entry in entries:
                if not isinstance(entry,dict):
                    raise protocol_error('CODEX_HISTORY_INCOMPLETE')
                inspect(entry.get('item'),entry.get('turnId'))
    else:
        raise protocol_error('CODEX_HISTORY_INCOMPLETE')
    if len(found)>1:
        raise protocol_error('CODEX_QUEUE_CONFLICT')
    return QueueEvidence(len(found),turn_id=found[0][0] if found else None,
        item_id=found[0][1] if found else None,outcome=found[0][2] if found else None,
        identity_kind=found[0][3] if found else None)


async def read_target_turn(client, thread_id, turn_id, *, schema=None, max_pages=500):
    """Read the exact persisted target, never substitute the most recent turn.

    Public read-only helper usable without an AgentStore. Null completion
    accounting remains unresolved even when an independent view says interrupted.
    """
    if not _id(thread_id) or not _id(turn_id):
        raise protocol_error('CODEX_HISTORY_INCOMPLETE')
    seen, found = set(), None
    async for turns in _pages(client, 'thread/turns/list', {'threadId':thread_id,
            'limit':20, 'sortDirection':'desc', 'itemsView':'notLoaded'}, schema, max_pages):
        for turn in turns:
            if (not isinstance(turn, dict) or not _id(turn.get('id')) or turn['id'] in seen
                    or turn.get('itemsView') != 'notLoaded' or turn.get('items') != []
                    or turn.get('status') not in {'inProgress','completed','failed','interrupted'}):
                raise protocol_error('CODEX_HISTORY_INCOMPLETE')
            seen.add(turn['id'])
            if turn['id'] == turn_id:
                found = turn
        if found is not None:
            return found
    return None
