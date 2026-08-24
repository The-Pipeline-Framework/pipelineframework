# Execution and replay

Read this reference only for external observations/effects/suspension, connectors,
aspects, persistence/replay authorities, resilience, retry/DLQ, or checkpoint handoff.
Verify exact configuration and release support in current docs/source/tests.

## Query, Command, and Await

Use Query for a genuinely new, current, or historical external observation: a database
fact, provider status, or model proposal. If the pipeline already knew the fact, carry
it. Query capture replays that observation; generic cache may bypass the whole step.

Treat model/browser/provider output as untrusted observation. Authoritative identifiers,
permissions, policy, and suspended request context stay in trusted pipeline/Await state.
The framework combines them; never ask the observer to echo authority correctly.

Use Command for one logical external effect: email, payment, archive, index, ticket, or
provisioning. The Command ID names the logical effect. Dispatch, execution, retry,
worker, and transport attempt IDs do not create new effect identities.
`CommandEffectStore` records effect authority/outcome. The provider still needs a stable
idempotency key or external identifier; TPF cannot manufacture exactly-once behavior
after an ambiguous third-party failure.

Use Await when the final answer arrives later: human approval, webhook callback,
brokered reply, or long-running provider result. Immediate request/response is Query or
operator territory. Transport may vary, but Await owns correlation, completion
admission, deadline/timeout, duplicate completion, durable request/completion snapshots,
and resume. Do not build a polling table or workflow registry beside it.

## Configure connectors by lifetime

Keep three scopes separate:

```text
provider configuration  -> connection/resource lifetime
operation configuration -> static choice for one Query/Command operation
invocation input         -> dynamic typed business value
```

Use a named connector binding to choose a provider instance and an operation to choose
one capability. Do not put dynamic business data in deployment configuration or put
credentials/runtime handles in canonical types. Operation config must not encode a
sequence of operations, branch predicates, or a second workflow language.

Implement the Connector SPI only for a reusable external boundary with stable provider
identity, typed configuration/outcomes, and lifecycle/resource ownership. Let TPF derive
descriptors, registration, validation, invocation context, retries, and metadata. Do not
write application provider factories or generic operation registries. A one-application
policy remains application code unless a missing framework semantic is proven.

## Keep state authorities distinct

| Mechanism | What it owns |
| --- | --- |
| typed pipeline state | facts known by this computation |
| persistence aspect | durable business values/history at typed boundaries |
| generic cache | versioned pipeline/step result reuse |
| Query capture | replay of an external observation |
| CommandEffectStore | logical effect identity and recorded outcome |
| Await storage | suspended interaction and completion/resume state |
| execution state | runtime progress, dispatch, retry, and lifecycle |

One database can host several authorities; shared storage does not merge them. An aspect
observes applicable typed boundaries orthogonally; it is not a hidden business step.
A cache hit does not prove a live observation or effect. Persistence does not authorize
an effect. Query capture does not replace Command idempotency. Await state does not
replace execution state or business history.

## Resilience and recovery

Backpressure limits how much live work flows. Timeout bounds one started invocation.
Retry/backoff decides whether a failure is attempted again. Circuit admission decides
whether a known-unhealthy managed invocation starts at all. Keep these policies in the
framework-owned shell.

Hidden outbound calls inside a service/operator cannot receive TPF-owned circuit,
retry, telemetry, correlation, or durability behavior. If the framework must own those
semantics, put the I/O behind a supported managed boundary.

Use typed outcomes for expected business failures, item rejection for supported
recover-and-continue cases, and execution failure/DLQ for systemic failure. Re-drive
must preserve execution/effect identity and use the current supported entrypoint; do
not create a retry ledger or invent a fresh effect identity.

## Checkpoint handoff

Checkpoint handoff is a reliable cross-pipeline ownership boundary, not a nested call
and not Await. The source publishes a typed checkpoint; the downstream pipeline admits
it through a framework-owned subscription/mapping boundary. Publication backlog and
admission failure occur before downstream execution. After admission, downstream
execution owns retry, DLQ, and lifecycle semantics. Preserve stable handoff identity;
do not make application code own broker correlation or duplicate admission.

Search the specific Query/Command/Await or connector authoring docs first, then compiler
descriptors, provider manifests, runtime support/stores, and focused capture/cache/
effect/duplicate/retry/completion tests. Check current execution-safety and checkpoint
docs before claiming circuit or handoff support for a particular boundary.
