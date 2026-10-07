import importlib
from dataclasses import replace
import pytest

from hpc_mcp.agent.store import AgentStore
from hpc_mcp.agent.delivery import completion_message
from hpc_mcp.errors import HpcMcpError


def protocol():
    assert importlib.util.find_spec('hpc_mcp.agent.queue_protocol') is not None, 'queue evidence API missing'
    return importlib.import_module('hpc_mcp.agent.queue_protocol')


def test_versioned_payload_and_digest(tmp_path):
    p = protocol()
    store = AgentStore(tmp_path/'state.db')
    w = store.register_watch(server_alias='fixture', job_id='12', project_id='project',
        thread_id='thread', session_id='session', cwd=str(tmp_path))
    e = store.record_terminal(w.id, 'COMPLETED', now=1)
    assert p.render_queue_message(w, e, version=1) == completion_message(w, e)
    message = p.render_queue_message(w, e)
    assert 'project_cwd' in message and 'AGENTS.md' in message and 'overwrite' in message
    assert len(p.message_digest(message)) == 64
    for cwd in ('relative', str(tmp_path/'missing'), str(tmp_path)+'\x01'):
        with pytest.raises(HpcMcpError):
            p.render_queue_message(replace(w, cwd=cwd), e)
    with pytest.raises(HpcMcpError):
        p.render_queue_message(w, e, version=99)


@pytest.mark.parametrize('turn,expected', [
    ({'id':'turn','status':'interrupted','completedAt':None}, None),
    ({'id':'turn','status':'completed','completedAt':1234}, 'COMPLETED'),
    ({'id':'turn','status':'failed','completedAt':1234}, 'FAILED'),
    ({'id':'turn','status':'interrupted','completedAt':1234}, 'INTERRUPTED'),
    ({'id':'turn','status':'completed','completedAt':float('nan')}, None),
    ({'id':'turn','status':'completed','completedAt':True}, None),
    ({'id':'turn','status':'completed','completedAt':2,'startedAt':3}, None),
    ({'id':'turn','status':{},'completedAt':1234}, None),
])
def test_terminal_accounting(turn, expected):
    assert protocol().terminal_outcome(turn) == expected


def item(id='item', client='event', text='message'):
    return {'id':id, 'type':'userMessage', 'clientId':client,
        'content':[{'type':'text','text':text,'text_elements':[]}]}


class Client:
    def __init__(self, pages): self.pages = iter(pages)
    async def request(self, method, params): return next(self.pages)


@pytest.mark.asyncio
async def test_full_identified_history_and_duplicates():
    p = protocol()
    thread = {'id':'thread', 'turns':[{'id':'turn', 'items':[item()], 'status':'completed','completedAt':1234}]}
    evidence = await p.read_identified_history(None, thread, 'event', 'message')
    assert evidence.exact_count == 1 and evidence.turn_id == 'turn'
    assert evidence.outcome == 'COMPLETED'
    for items, code in [([item(),item('second')], 'CODEX_QUEUE_CONFLICT'),
                        ([item(text='wrong')], 'CODEX_QUEUE_CONFLICT'),
                        ([item(client=None)], 'CODEX_HISTORY_IDENTITY_UNAVAILABLE')]:
        with pytest.raises(HpcMcpError) as error:
            await p.read_identified_history(None, {'id':'thread','turns':[{'id':'turn','items':items}]}, 'event','message')
        assert error.value.code == code


@pytest.mark.asyncio
async def test_paginated_history_rejects_incomplete_repeated_and_missing_turn():
    p = protocol()
    thread = {'id':'thread','turns':[], 'historyMode':'paginated'}
    valid = {'item':item(),'turnId':'turn'}
    ev = await p.read_identified_history(Client([{'data':[valid], 'nextCursor':None}]),thread,'event','message')
    assert ev.turn_id == 'turn'
    for pages in ([{'data':[{'item':item()}],'nextCursor':None}],
                  [{'data':[valid],'nextCursor':'next'},{'data':[valid],'nextCursor':None}],
                  [{'data':[],'nextCursor':'next'},{'data':[],'nextCursor':'next'}]):
        with pytest.raises(HpcMcpError):
            await p.read_identified_history(Client(pages),thread,'event','message')


@pytest.mark.asyncio
async def test_queue_exact_identity_and_conflicting_duplicates():
    p = protocol()
    entry = {'id':'queue','clientUserMessageId':'event','input':[{'type':'text','text':'message','text_elements':[]}]}
    ev = await p.read_queue_evidence(Client([{'data':[entry],'nextCursor':None}]), 'thread','event','message')
    assert ev.exact_count == 1 and ev.queue_id == 'queue'
    for data in ([entry, dict(entry,id='other')], [dict(entry,input=[])], [dict(entry,clientUserMessageId=None)]):
        with pytest.raises(HpcMcpError):
            await p.read_queue_evidence(Client([{'data':data,'nextCursor':None}]), 'thread','event','message')


def test_queue_schema_independent_of_direct_start():
    p = protocol()
    from hpc_mcp.agent.app_server import ProtocolSchema
    from tests.unit.test_app_server import schema_bundle
    bundle = schema_bundle()
    bundle['definitions']['ThreadTurnsListParams']['properties']['cursor'] = {'type':'string'}
    bundle['oneOf'] = [b for b in bundle['oneOf'] if b['properties']['method']['enum'][0] != 'turn/start']
    for method, fields in [('thread/queue/add',['threadId','clientUserMessageId','input']),
                           ('thread/queue/list',['threadId','cursor','limit'])]:
        bundle['oneOf'].append({'type':'object','properties':{'id':{'type':'integer'},
            'method':{'enum':[method]},'params':{'type':'object','properties':{f:{} for f in fields}}}})
    schema = ProtocolSchema(bundle, {'type':'object'}, route='queue')
    p.require_queue_schema(schema)
    schema.methods['thread/queue/add']['properties']['input']={'type':'invalid-type'}
    with pytest.raises(HpcMcpError): p.require_queue_schema(schema)
    schema.methods['thread/queue/add']['properties']['input']={}
    del schema.methods['thread/resume']['properties']['excludeTurns']
    with pytest.raises(HpcMcpError): p.require_queue_schema(schema)
    p.require_queue_schema(schema,resume_enabled=False)
    del schema.methods['thread/queue/add']
    with pytest.raises(HpcMcpError): p.require_queue_schema(schema)


def test_context_digest_excludes_mutable_watch_state(tmp_path):
    p = protocol()
    store = AgentStore(tmp_path/'state.db')
    w = store.register_watch(server_alias='fixture',job_id='12',project_id='project',
        thread_id='thread',session_id='session',cwd=str(tmp_path))
    e = store.record_terminal(w.id,'COMPLETED',now=1)
    assert p.context_digest(w,e) == p.context_digest(replace(w,last_state='COMPLETED',active=False),e)


@pytest.mark.asyncio
async def test_doctor_selects_queue_schema(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    from tests.unit.setup.test_install import service
    instance, _, _ = service(tmp_path, monkeypatch)
    calls=[]
    async def schema(**kwargs): calls.append(kwargs)
    monkeypatch.setattr(install,'generate_schema',schema)
    assert (await instance.doctor())['app_server'] == 'compatible'
    assert calls == [{'route':'queue'}]


def test_reconstruction_and_ack_are_exact(tmp_path):
    p=protocol()
    from hpc_mcp.agent.queue_store import QueueStore
    store=AgentStore(tmp_path/'state.db')
    w=store.register_watch(server_alias='fixture',job_id='12',project_id='project',
        thread_id='thread',session_id='session',cwd=str(tmp_path))
    e=store.record_terminal(w.id,'COMPLETED',now=1)
    e=store.claim_event('worker',now=2)
    message=p.render_queue_message(w,e)
    receipt=QueueStore(store).begin_intent(e,'worker',2,p.message_digest(message),p.context_digest(w,e),now=3)
    assert p.reconstruct_message(w,e,receipt) == message
    for changed in (replace(receipt,message_sha256='a'*64),replace(receipt,format_version=99)):
        with pytest.raises(HpcMcpError): p.reconstruct_message(w,e,changed)
    entry={'id':'queue','clientUserMessageId':e.id,'input':[{'type':'text','text':message,'text_elements':[]}]}
    assert p.validate_queue_ack({'queuedSubmission':entry},e.id,message) == 'queue'
    with pytest.raises(HpcMcpError):
        p.validate_queue_ack({'queuedSubmission':dict(entry,clientUserMessageId='wrong')},e.id,message)
