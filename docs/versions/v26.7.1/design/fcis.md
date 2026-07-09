---
search: false
---

# Functional Core, Imperative Shell

TPF works best when the business core stays boring and explicit:

- a step receives a typed input,
- applies a business transformation,
- returns a typed output,
- and avoids owning transport, persistence, retries, correlation, polling, or deployment wiring.

That is the functional core. It does not mean every Java method is mathematically pure. It means business decisions are kept separate from the shell code that connects the application to external reality.

## The Shell

The shell is where distributed-system concerns live.

| Concern | TPF surface |
| --- | --- |
| Expose or call a step over REST, gRPC, local, or function entry points | generated transport adapters |
| Admit files, object-store entries, or external payloads | connectors such as [Object Ingest](/versions/v26.7.1/design/object-ingest) |
| Wait for human approval, webhook callbacks, provider responses, or long-running jobs | [Await Boundaries](/versions/v26.7.1/design/await-boundaries) |
| Record and replay-safe external effects such as indexing, tickets, emails, or provisioning | [Command Steps](/versions/v26.7.1/deploy/orchestrator-runtime/command) |
| Call a synchronous existing method or remote request/response endpoint | [Operators](/versions/v26.7.1/design/operators) |
| Store business outputs for later query or audit | [Persistence](/versions/v26.7.1/design/persistence) |
| Reuse expensive derived outputs | [Caching](/versions/v26.7.1/design/caching/) |
| Hand off stable output to a separately owned downstream pipeline | [Checkpoint Handoff](/versions/v26.7.1/deploy/orchestrator-runtime/checkpoint-handoff) |

## Design Rule

If the code is making a domain decision, keep it in a typed step.

If the code is handling I/O admission, correlation, retries, lifecycle state, payload references, replay, or transport shape, move it into a TPF shell surface when one exists.

## Examples

Object ingest keeps directory scans, S3 listings, ETags, duplicate admission, and payload references out of a parser step. The parser receives a typed input and parses.

Await keeps pending interaction state, resume tokens, timeout handling, duplicate completion, and callback admission out of a business approval step. The step sees an explicit typed outcome.

Command keeps effect ids, recorded outputs, duplicate policy, retry classification, and DLQ state out of a business projection step. The connector executes the external write, but the pipeline owns the effect boundary.

Checkpoint handoff keeps downstream admission, handoff idempotency, and publication envelopes out of application bridge code. The source pipeline publishes a stable typed checkpoint; the target pipeline receives a typed input.

## What TPF Does Not Hide

TPF does not remove architectural decisions. It makes them explicit:

- contracts are named and typed,
- boundary policies are configured,
- generated transports are visible,
- deployment mode is separate from business logic,
- operational state is observable.

That is the point of "Keep the core pure. Connect to reality."
