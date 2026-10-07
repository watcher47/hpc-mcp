# Delivery attempt diagnostics

The current CLI uses the [native queue handoff](native-queue-handoff.md).
The bounded journal fields below apply to both routes; the direct-start readiness
sections are historical. Synthetic source checks do not establish live queue
acceptance.

`hpc-agent status` includes a compact diagnostic summary for each event.
`hpc-agent diagnostics --event-id <event-id> --limit 20` reads recent attempts
without starting, resuming or steering a Codex turn. The limit is 1 through 100;
the newest 200 detailed attempts are retained per event.

The response gives `total_attempt_count`, `retained_count`, `returned_count` and
`gap_count`. A gap includes attempts predating the journal and details removed by
retention. It does not indicate a particular error or cause. Existing historical
attempts, including the earlier 105 attempts, cannot be reconstructed and are
never fabricated.

Each delivery claim atomically records an attempt. Current queue admission uses
schema, initialization, queue preflight, enqueue and queued stages. Historical
direct-start attempts used first/second history, readiness and send acknowledgement.
Deferred, cancelled, delivered and manual-recovery transitions finish the attempt
in the same lease-fenced transaction as the event transition. Reclaiming an
expired lease marks its existing unfinished attempt abandoned. Every write is
fenced to the current attempt; accepted cleanup also requires the exact original
attempt owner and number.

`cleanup_state`, `cleanup_updated_at` and `cleanup_error_code` describe teardown
separately, preserving the stage/error that caused a delivery failure. Cleanup
diagnostic storage failures never prevent actual owned child cleanup. `RUNNING`
cleanup can remain after a crash; it does not prove a child still exists.

Only fixed stages, outcomes, local error codes, known RPC methods and signed
32-bit integer RPC error codes are retained. Booleans are not integer error
codes. Exception messages, RPC error messages/data, traceback text and request
contents are not stored in this journal. Unknown local error codes normalize to
`CODEX_HANDOFF_FAILED`, including secret-like values beginning with `CODEX_`.

The journal is an additive table under database schema version 3. Migration does
not modify completed event receipts. Compatible older releases can still read
and write their existing tables, but do not maintain this journal; gaps and
unfinished audit records during rollback are possible. Current event and queue
receipts record delivery/outcome evidence separately from historical attempts.
Queue observations do not create journal attempts: only an exact identified input
proves delivery, and its own persisted turn with valid nonnull completion
accounting proves a final outcome. Fresh proof clears the event's obsolete
delivery error; the journal retains the earlier failure. Diagnostic rows never
authorize a retry.

## Historical direct-start receipts

Cancellation after send preserves `send_started_at`. Later processing must
reconcile identified history; uncertain sends require manual recovery if no
proof is found. `DELIVERED` means an accepted input, while `handoff_state` records
the streamed turn outcome separately. Neither diagnostics nor a delivered
receipt prove artifact collection completed or global desktop idleness.

## Historical direct-start readiness gate

For never-sent events, the `READINESS` stage requires a newest persisted turn
with a nonempty ID and `completed` status before resume. After the second full
history inspection, the same completed ID must still be newest immediately
before durable send intent. Both observations use generated-schema-validated
`thread/turns/list` with `limit=1`, `sortDirection=desc`, `itemsView=notLoaded`.
Runtime thread/session checks still apply. `notLoaded` alone never proves idle.
This endpoint is required even when marker history uses the legacy full read;
unsupported older schemas defer instead of guessing legacy turn ordering.

`CODEX_THREAD_NOT_READY` means a persisted in-progress, interrupted or failed
turn. `CODEX_THREAD_CHANGED` means the completed identity changed between
observations. These observations and runtime `CODEX_THREAD_ACTIVE` retry after
60 seconds on every attempt. `CODEX_READINESS_UNAVAILABLE` means missing or
incompatible query support, or empty/malformed/unknown latest-turn evidence;
it retains bounded protocol backoff. None creates send intent or starts a turn.
The first failed readiness check also prevents resume. Exact identified history
reconciliation has precedence: accepted inputs are acknowledged without another
send, and an uncertain prior send without proof still requires manual recovery,
even when readiness is unavailable.

Interrupted, failed and unknown latest states may leave a never-sent event
pending. A later completed ordinary user turn can establish new evidence, or an
operator can stop the agent and use `manual-recovery` in the original task after
checking history and activity. Accepted or uncertain sends must never be pasted
again. Readiness diagnostics contain no transcript or turn text.

Two matching reads are an observation, not a global lock or atomic idle-only
start. Another process can start after the final check, and persisted history
may lag live activity. The reviewed legacy start schema had no idle-only/CAS
condition; `turn/start` can implicitly steer an active turn. Queue request shapes
alone did not establish safe cross-process scheduling semantics, so this
historical gate did not use them or claim universal noninterference.
Controlled live acceptance needs
its separate original-controller completion boundary; missing safe evidence
must preserve pending state. Monitor timing and watch/submission identities
are unchanged.
