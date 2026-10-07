"""Enqueue once; reconcile exact input and outcome through durable observations."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import uuid4

from .app_server import generate_schema, protocol_error
from .delivery import DeliveryService, _thread, completion_message, logger
from .queue_protocol import (context_digest, message_digest, read_identified_history,
    read_queue_evidence, read_target_turn, reconstruct_message, render_queue_message,
    require_queue_schema, terminal_outcome, validate_queue_ack)
from .queue_store import FINAL, QueueStore
from .store import safe_delivery_code
from hpc_mcp.errors import HpcMcpError


async def queue_schema_loader():
    return await generate_schema(route='queue')


class QueueDeliveryService(DeliveryService):
    """Use the existing bounded batch and own-child lifecycle, with no direct start."""
    def __init__(self, store, *, schema_loader=queue_schema_loader,
                 observation_interval=50, ownership_timeout=86400, **kwargs):
        super().__init__(store, schema_loader=schema_loader, **kwargs)
        self.queue = QueueStore(store)
        self.observation_interval = observation_interval
        self.ownership_timeout = ownership_timeout
        self._owned_events = set()
        self._prefer_observation = True
        # One transient client at a time; nineteen retained clients leave one
        # reader slot for terminal proof. Never wait with a claimed DB lease.
        self._operation_lock = asyncio.Lock()

    async def deliver_once(self):
        if self._operation_lock.locked() or len(self._owned_sessions) >= 19:
            return False
        async with self._operation_lock:
            return await super().deliver_once()

    async def _deliver_batch(self, limit):
        for _ in range(min(limit, 20)):
            first = self.observe_once if self._prefer_observation else self.deliver_once
            second = self.deliver_once if self._prefer_observation else self.observe_once
            self._prefer_observation = not self._prefer_observation
            if not await first() and not await second():
                break

    async def _inventory(self, client, schema, watch, event, message, receipt=None):
        acknowledged = receipt is not None and receipt.has_confirmed_ack
        queue_id = receipt.queue_id if acknowledged else None
        params = {'threadId': watch.thread_id, 'includeTurns': False}
        schema.validate('thread/read', params)
        thread = _thread(await client.request('thread/read', params), watch)
        queued = await read_queue_evidence(client, watch.thread_id, event.id, message, schema=schema,
            acknowledged_queue_id=queue_id)
        if thread.get('historyMode', 'legacy') == 'legacy':
            params['includeTurns'] = True
            schema.validate('thread/read', params)
            thread = _thread(await client.request('thread/read', params), watch)
        history = await read_identified_history(client, thread, event.id, message, schema=schema,
            acknowledged_null_client=acknowledged)
        if queued.exact_count and history.exact_count:
            # Queue consumption between independent reads is normal. A surviving
            # exact entry needs manual reconciliation; never acknowledge overlap.
            again = await read_queue_evidence(client, watch.thread_id, event.id, message, schema=schema,
                acknowledged_queue_id=queue_id)
            if again.exact_count:
                if again.queue_id != queued.queue_id:
                    raise protocol_error('CODEX_QUEUE_CONFLICT')
                raise protocol_error('CODEX_QUEUE_TRANSITION_PENDING')
            queued = again
        if history.exact_count and thread.get('historyMode') == 'paginated':
            turn = await read_target_turn(client, watch.thread_id, history.turn_id, schema=schema)
            history = replace(history, outcome=terminal_outcome(turn))
        return thread, queued, history

    async def _deliver(self, event, owner):
        watch = self.store.get_watch(event.watch_id)
        if watch is None:
            raise protocol_error('CODEX_EVENT_CONTEXT_INVALID')
        # Frozen v1 identity for legacy uncertain sends, forever read-only.
        legacy = event.send_started_at is not None
        message = completion_message(watch, event) if legacy else render_queue_message(watch, event)
        self._stage(event, owner, 'SCHEMA')
        schema = await self.schema_loader()
        require_queue_schema(schema)
        payload = {'threadId':watch.thread_id, 'clientUserMessageId':event.id,
            'input':[{'type':'text', 'text':message, 'text_elements':[]}]}
        schema.validate('thread/queue/add', payload)
        manager = self.client_factory(schema)
        self._stage(event, owner, 'INITIALIZATION')
        try:
            client = await manager.__aenter__()
            self._stage(event, owner, 'QUEUE_PREFLIGHT')
            _, queued, history = await self._inventory(client, schema, watch, event, message)
            if legacy:
                if history.exact_count:
                    self.store.mark_delivered(event.id, owner, now=self.clock(), turn_id=history.turn_id,
                        handoff_state=history.outcome or 'UNKNOWN')
                else:
                    self.store.require_manual_recovery(event.id, owner, now=self.clock(),
                        code='CODEX_SEND_OUTCOME_UNKNOWN')
                return
            if history.exact_count or queued.exact_count:
                # Evidence already exists without a durable local intent. Do not
                # create a false send authorization or guess a renderer version.
                self.store.require_manual_recovery(event.id, owner, now=self.clock(),
                    code='CODEX_QUEUE_CONFLICT')
                return
            self._stage(event, owner, 'ENQUEUE')
            receipt = self.queue.begin_intent(event, owner, 2, message_digest(message),
                context_digest(watch, event), now=self.clock())
            if receipt is None:
                raise protocol_error('CODEX_LEASE_EXPIRED')
            response = await client.request('thread/queue/add', payload)
            queue_id = validate_queue_ack(response, event.id, message)
            self._stage(event, owner, 'QUEUED')
            if not self.queue.record_ack(receipt, owner, queue_id, now=self.clock()):
                raise protocol_error('CODEX_LEASE_EXPIRED')
        finally:
            self._cleanup_evidence(event, owner, 'RUNNING')
            confirmed = await self._close_manager(manager)
            self._cleanup_evidence(event, owner, 'CONFIRMED' if confirmed else 'UNCONFIRMED')
            if not confirmed:
                # Do not lose ownership of a child whose exit was not confirmed.
                token = object()
                self._owned_sessions[token] = (manager, event, None, owner)
                self._owned_events.add(event.id)

    async def observe_once(self, *, event_id=None):
        if self._operation_lock.locked():
            return False
        async with self._operation_lock:
            return await self._observe_once(event_id=event_id)

    async def _observe_once(self, *, event_id=None):
        owner = str(uuid4())
        receipt = None
        try:
            async with asyncio.timeout(100):
                if len(self._owned_sessions) >= 20:
                    # A failed reader close may occupy the reserved slot.
                    # Retry one cleanup-only child; never evict a live owner
                    # to make room, or open another child without confirmed exit.
                    for token, (manager, event, turn_id, prior_owner) in list(self._owned_sessions.items()):
                        if not isinstance(token, asyncio.Task) or token.done():
                            await self._release_session(token, manager, event, turn_id,
                                'UNKNOWN', 'CODEX_SESSION_INTERRUPTED', prior_owner)
                            if token in self._owned_sessions:
                                # Give other cleanup-only children a chance on
                                # the next attempt if this exit stays unconfirmed.
                                retained = self._owned_sessions.pop(token)
                                self._owned_sessions[token] = retained
                            break
                    if len(self._owned_sessions) >= 20:
                        return False
                receipt = self.queue.claim_observation(owner, now=self.clock(), event_id=event_id)
                if receipt is None:
                    return False
                await self._observe(receipt, owner)
        except asyncio.CancelledError:
            if receipt is not None:
                self._observation_error(receipt, owner, 'CODEX_SESSION_INTERRUPTED')
            raise
        except HpcMcpError as error:
            self._observation_error(receipt, owner, error.code)
        except (OSError, TimeoutError):
            self._observation_error(receipt, owner, 'CODEX_UNAVAILABLE')
        except Exception:
            self._observation_error(receipt, owner, 'CODEX_HANDOFF_FAILED')
        return receipt is not None

    def _observation_error(self, receipt, owner, code):
        if receipt is None:
            return
        conflict = code in {'CODEX_QUEUE_IDENTITY_INVALID', 'CODEX_QUEUE_CONFLICT',
            'CODEX_HISTORY_IDENTITY_UNAVAILABLE'}
        if code == 'CODEX_QUEUE_TRANSITION_PENDING' and receipt.last_error == code:
            conflict, code = True, 'CODEX_QUEUE_CONFLICT'
        self.queue.record_observation(receipt, owner, now=self.clock(),
            state='CONFLICT' if conflict else None, error_code=safe_delivery_code(code))

    async def _observe(self, receipt, owner):
        event = next(e for e in self.store.events() if e.id == receipt.event_id)
        watch = self.store.get_watch(event.watch_id)
        if watch is None:
            raise protocol_error('CODEX_QUEUE_IDENTITY_INVALID')
        try:
            message = reconstruct_message(watch, event, receipt)
        except HpcMcpError:
            raise protocol_error('CODEX_QUEUE_IDENTITY_INVALID') from None
        schema = await self.schema_loader()
        require_queue_schema(schema)
        manager = self.client_factory(schema)
        transferred = False
        try:
            client = await manager.__aenter__()
            thread, queued, history = await self._inventory(client, schema, watch, event, message, receipt)
            if history.exact_count:
                self.queue.record_observation(receipt, owner, now=self.clock(),
                    state=history.outcome or 'OBSERVING', turn_id=history.turn_id,
                    item_id=history.item_id, identity_kind=history.identity_kind)
            elif receipt.state == 'CONFLICT':
                self.queue.record_observation(receipt, owner, now=self.clock(),
                    error_code='CODEX_SEND_OUTCOME_UNKNOWN')
            elif queued.exact_count:
                if receipt.turn_id or (receipt.queue_id and receipt.queue_id != queued.queue_id):
                    raise protocol_error('CODEX_QUEUE_CONFLICT')
                # Independent notLoaded does not establish absence of a writer.
                # Native writer refusal is authoritative and is never bypassed.
                can_resume = (thread['status'].get('type') == 'notLoaded'
                    and event.id not in self._owned_events and len(self._owned_sessions) < 19)
                if can_resume:
                    params = {'threadId':watch.thread_id, 'excludeTurns':True}
                    schema.validate('thread/resume', params)
                    _thread(await client.request('thread/resume', params), watch)
                    # Ownership exists from successful resume, even if CAS later
                    # loses its lease. Keep the child supervised, never discard it.
                    self._owned_events.add(event.id)
                    task = asyncio.create_task(self._supervise_queue(manager, client, event, owner))
                    self._sessions.add(task)
                    self._owned_sessions[task] = (manager, event, None, owner)
                    task.add_done_callback(self._observe_task)
                    transferred = True
                self.queue.record_observation(receipt, owner, now=self.clock(), state='QUEUED',
                    queue_id=queued.queue_id, next_observe_at=self.clock() if transferred else None)
            else:
                self.queue.record_observation(receipt, owner, now=self.clock(),
                    error_code='CODEX_SEND_OUTCOME_UNKNOWN')
        finally:
            if not transferred:
                confirmed = await self._close_manager(manager)
                if not confirmed:
                    token = object()
                    self._owned_sessions[token] = (manager, event, None, owner)
                    self._owned_events.add(event.id)
                    self.store.record_cleanup_failure(event.id)

    async def _supervise_queue(self, manager, client, event, owner):
        drain = asyncio.create_task(client.drain_notifications())
        code = 'CODEX_SESSION_INTERRUPTED'
        try:
            async with asyncio.timeout(self.ownership_timeout):
                while True:
                    receipt = self.queue.get(event.id)
                    if receipt.state in FINAL:
                        break
                    if drain.done():
                        drain.result()
                        raise protocol_error('CODEX_CONNECTION_LOST')
                    await self.observe_once(event_id=event.id)
                    await asyncio.sleep(self.observation_interval)
        except HpcMcpError as error:
            code = safe_delivery_code(error.code)
        except (OSError, TimeoutError):
            pass
        finally:
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
            await self._release_session(asyncio.current_task(), manager, event, None,
                'UNKNOWN', code, owner)

    async def _release_session(self, task, manager, event, turn_id, state, code, owner):
        # A stopped owner leaves an unknown outcome; a passive observer never
        # enters this path. Fresh exact observations reconcile it after restart.
        try:
            # Only supervisor tasks own resumed execution. Retained cleanup-only
            # tokens from passive/notifier clients must not change turn outcome.
            if isinstance(task, asyncio.Task):
                receipt = self.queue.get(event.id)
                if receipt is not None:
                    self.queue.record_owner_stop(receipt, now=self.clock(), error_code=code)
        except Exception:
            logger.error('CODEX_HANDOFF_PERSISTENCE_FAILED; inspect hpc-agent diagnostics')
        cleanup = asyncio.create_task(self._close_owned(task, manager, event, owner))
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise
        if not any(owned_event.id == event.id for _, owned_event, _, _ in self._owned_sessions.values()):
            self._owned_events.discard(event.id)
