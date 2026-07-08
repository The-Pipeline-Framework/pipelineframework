---
search: false
---

# State Model

TPF uses several state surfaces. They solve different problems and should not be treated as interchangeable storage.

| Surface | Purpose | Use when |
| --- | --- | --- |
| Persistence | Durable business records | APIs, reports, UIs, audit, or queryable outputs need saved domain data |
| Cache | Reusable derived outputs | Expensive deterministic work should be reused during normal execution or replay |
| Materialization | Large payload claim-checks | Payloads are too large or awkward to carry inline through every boundary |
| Execution state | Runtime progress and recovery | `QUEUE_ASYNC` needs leases, retry timing, terminal status, or crash recovery |
| Await units | Durable external waiting | A flow must pause for callbacks, approvals, provider decisions, or long-running jobs |
| Checkpoint handoff | Cross-pipeline admission | One pipeline finishes a stable output and another pipeline owns the next flow |

## Decision Path

1. Query business data later? Use [Persistence](/versions/v26.6.2/design/persistence).
2. Want to avoid recomputing a deterministic output? Use [Caching](/versions/v26.6.2/design/caching/).
3. Carrying large content across boundaries? Use [Field Materialization](/versions/v26.6.2/design/materialization).
4. Should work survive crashes? Use [Queue-Async Runtime](/versions/v26.6.2/deploy/orchestrator-runtime/queue-async).
5. Waiting for external reality? Use [Await Boundaries](/versions/v26.6.2/design/await-boundaries).
6. Handing off stable ownership to another pipeline? Use [Checkpoint Handoff](/versions/v26.6.2/deploy/orchestrator-runtime/checkpoint-handoff).

## Replay Boundary

Replay is easier when each state surface has a single job.

Persistence is the durable business record. Cache is an optimization and replay accelerator. Execution state is runtime bookkeeping. Await state records external waiting and completion snapshots. Checkpoint handoff records a stable boundary between two pipeline owners.

When these responsibilities are mixed in application code, retries and replay become harder to reason about. Keep them explicit.
