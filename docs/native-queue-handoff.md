# Native queue completion handoff

## Originating task accessibility and never-sent recovery

`hpc-agent watch` first performs a bounded read-only native `thread/read` with
`includeTurns: false`, using the real terminal thread/session IDs. Exact task,
session and cwd ancestry must match before the watch is persisted. A durable
task delegated to a Windows executor is not thereby a local native task. The
exact native `-32600 / thread not loaded: <requested ID>` response is classified
as `CODEX_THREAD_UNAVAILABLE`; arbitrary error text is neither exposed nor kept.
The service does not guess a different thread or bypass authentication.

For a previously misbound completion that has never had a send, an explicitly
authorized `hpc-agent recover-watch --server ... --job-id ... --project-id ...
--event-id ... --from-thread ...` may be run in the intended actual local task's
terminal, inside the registered project. It uses that terminal's verified native
context; there are no target thread/session flags. One SQLite transaction requires
the exact old thread, one inactive watch and one PENDING terminal event, no lease,
no legacy send fence and no queue receipt of any state. It records the old/new
destination in `watch_context_recoveries`, changes only the watch destination and
makes the same event due. JobID, event UUID, attempts and all diagnostic/send
evidence are retained. An identical retry reads the existing audit without
resetting or replaying anything. Any accepted/uncertain send, receipt, lease,
different recovery or ambiguous event refuses the operation. `diagnostics`
exposes the bounded audit. This command never submits or cancels a Slurm job.

Automatic delivery still must prove the exact input and that input's own final
turn accounting and result processing; registration or queue ACK alone is not
acceptance.

## Foreground profiles and background configuration

If the foreground MCP can use an alias but the background agent reports
`CONFIG_NOT_FOUND`, inspect the effective configuration path in both execution
contexts. Packaged Windows apps can resolve a logical AppData path to an MSIX
LocalCache file. Keep configuration contents private. Reapply the verified wheel
through the managed installer with the effective path so registration, startup
and installation metadata agree; preserve current watches and send fences.

Startup ownership still requires the exact user SID, managed marker, action,
trigger and privilege checks. Native command output must retain localized account
names even when Python uses UTF-8 mode. See [Windows setup](windows-setup.md)
for managed installation and rollback, and [GPU selection](gpu-selection.md)
for the submission and allocation guards.

The agent admits a completion event through `thread/queue/add` once, using its
existing event UUID as `clientUserMessageId`. It records the version, message and
registered-context digests, and a durable send fence before the request. An ACK
means `QUEUED`; it does not prove the input ran or artifact collection finished.

Read-only observations scan the complete identified history once per bounded
client attempt. One exact identified input records delivery. Only that input's
own persisted turn can supply a final outcome: completed, failed or interrupted
requires valid nonnull completion accounting. An independent interrupted view
with null accounting remains unresolved. The latest unrelated turn, an empty
queue, child exit and ACK are never substitutes for completion evidence.

The strict native `clientId` route remains the default. Some Desktop versions
consume a queued message with null or absent `clientId`. For a reconstructable
v2 receipt with a confirmed native queue ACK, a complete bounded history scan
may instead prove one byte-exact full message with one native item and turn ID.
The acknowledged queue ID must be absent; changed or replacement entries,
wrong nonnull client IDs, duplicate inputs, missing ACKs and incomplete scans
fail closed. Diagnostics distinguish `NATIVE_CLIENT_ID` from
`ACK_MESSAGE_NATIVE_ITEM`; the latter never invents a client UUID.

The first proof binds its item, turn and message/context digests in the additive
`queue_input_bindings` table. Existing completed rows and `queue_handoffs`
columns stay unchanged, so rollback readers remain compatible and send fences
remain in force. A narrowly eligible prior ACK-backed identity-unavailable
conflict can be leased for ordinary read-only observation. Only fresh unique
proof commits delivery under that lease; it never resets send authorization,
resumes a conflicted thread or adds again. Successful recovery retains the
prior identity diagnostic and existing attempt journal.

Historical-conflict reproof preserves its eligible
`CODEX_HISTORY_IDENTITY_UNAVAILABLE` origin in `queue_reproof_origins`, captured
under the observation lease before a read can replace the latest error. With
that origin and the existing receipt/context eligibility intact, only
`CODEX_UNAVAILABLE`, `CODEX_TIMEOUT`, `CODEX_CONNECTION_LOST`,
`CODEX_REQUEST_FAILED` and `CODEX_SESSION_INTERRUPTED` permit another read-only
reproof at the normal 50 second observation cadence. The finite allowlist does
not include schema incompatibility, incomplete history, other permanent errors
or conflicting evidence; those remain fenced for operator attention. A transient
failure preserves `CONFLICT`/`MANUAL_REQUIRED`; only fresh unique input proof can
recover delivery. Neither origin preservation nor retry authorizes another
enqueue or a resume of the conflicted thread. Diagnostics retain the origin as
`prior_identity_error` separately from `last_error`, including after recovery.

Desktop can consume the input into an already active turn. The agent does not
guarantee idle-only execution. Binding to that exact target still requires its
explicit final completion accounting before reporting a final handoff outcome.

Initial admission and due observations alternate in a bounded background batch.
The model-free Slurm monitor keeps its 45–55 second cadence. Persisted observations
are normally due every 50 seconds, so final reports can lag actual native
completion by a polling interval. An initial attempt has a 100 second budget and
a 120 second lease. Every client keeps the 16 MiB aggregate and 4 MiB JSONL line
limits; history/queue/turn scans have a 500 page ceiling. Bounds failures preserve
uncertainty and never silently truncate history or permit another add.

For an exact queued entry, independent `notLoaded` permits a bounded resume-only
attempt with `excludeTurns: true` and no cwd override. This does not prove that
an existing native writer is absent. Native refusal defers to that writer.
Successful resume retains and drains the owned child, while separately leased
observations establish exact input and terminal evidence. The agent owns at most
20 clients in total, including temporary readers and children retained after
unconfirmed cleanup. It admits at most 19 retained clients through new work or
resume, reserving one slot for a reader to collect final proof. Admission and
observation operations share a nonwaiting lock acquired before any lease; busy
calls defer without claiming work. A failed reader cleanup may fill the last
slot. At saturation, observations retry one cleanup-only child within their
100 second budget before claiming a lease or opening another client. Failed
cleanup retries rotate so one unreapable child does not starve other cleanup.
If no exit can be confirmed, observation waits for capacity; it never opens a
twenty-first child or stops a live owner to create space.
The agent supervises each resumed owner for at most 24 hours. Shutdown, timeout or
stream failure records uncertainty and reaps only its own child. It never calls
`turn/start`, `turn/steer`, `thread/queue/start`, or deletes queue entries.
When an identified owned turn loses its owner before final proof, its handoff
outcome becomes UNKNOWN with no final-completion timestamp, including when a
conflict stops its owner and leaves delivery marked MANUAL_REQUIRED. The conflict
diagnostic and verified terminal outcomes are preserved. Ordinary passive
observer close does not change that field; fresh exact evidence can repair it.

`hpc-agent status` and `diagnostics` expose safe queue receipts: state, queue and
turn IDs, version/digests, timestamps, leases/revision and finite error codes.
No message, transcript, raw RPC error or provider configuration is included in
those additions. `manual-recovery` reconstructs the persisted message version;
after intent, do not paste or requeue that input. A lost ACK, crash before socket
write or disappearance without exact history remains fenced and is observed
without replay. This trades automatic recovery in the crash-before-send window
for protection against duplicate requests.

Unexpected exact evidence for a locally never-sent event without a queue receipt
requires manual reconciliation; it does not invent a local intent or add again.
Queue/history overlap is re-inventoried; a remaining identical entry defers one
observation, and repeated overlap becomes a conflict. Duplicate queue IDs,
conflicting content/identity and renderer/context digest changes require manual
attention. External actors can enqueue the same client ID more than once: the
native protocol does not provide application-wide exactly-once delivery.
An observed queue ID must match a previously persisted queue ID before any
resume attempt; a replacement queue entry requires manual reconciliation.

The additive queue table keeps database version 3. Historical completed legacy
rows are untouched. Legacy uncertain sends retain the byte-identical v1 message
and reconcile read-only without entering the new route. Rollback cannot replay
a queue intent because the old send fence remains set; old code does not observe
the new queue state. A later upgrade resumes evidence collection. An incompatible
installed experimental schema fails closed, without a direct-start fallback.
Native process crashes during execution and Desktop-specific behavior remain
runtime acceptance limitations; synthetic tests do not establish deployment
acceptance.
