---
title: Await, Command, and Checkpoint Settings
---

# Await, Command, and Checkpoint Settings

Await, command, and checkpoint configuration belongs to the runtime shell. It keeps human approvals, webhook callbacks, long-running provider responses, replay-safe external effects, and cross-pipeline handoff out of business functions.

## Main Surfaces

| Surface | Purpose | Full reference |
| --- | --- | --- |
| Queue-async execution | durable background execution and retry ownership | [Orchestrator Background Execution](/develop/configuration/all-settings#orchestrator-background-execution) |
| Await transports | pending interaction and completion admission wiring | [Await Transports](/develop/configuration/all-settings#await-transports) |
| Command connectors | connector endpoint, credentials, timeout, provider retry tuning, and effect-store implementation | [Command Steps](/deploy/orchestrator-runtime/command) |
| Checkpoint handoff bindings | publication targets and subscriber admission | [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff) |
| Function handler context | context attributes captured at function boundaries | [Function Transport Context Attributes](/develop/configuration/all-settings#function-transport-context-attributes-function-handlersadapters) |

## Authoring an Await that can resume elsewhere

Put `await:` on the operation that produces the trusted request. Declare the reply and final output as canonical types. Use a deterministic projector when the final output depends on both the request and reply. The projector receives the retained request and admitted evidence, so it should avoid external effects and fresh reads.

Keep transport settings in the runtime configuration and use references for secrets. The proposed independently packaged mode uses compiler-emitted boundary facts and keeps the transport configuration fingerprint in the compiled contract. Check the pinned release for supported transports and layout. [Await architecture](/architecture/await-boundaries#proposed-separation-of-coordination-and-typed-execution)
## Related Guides

- [Await Boundaries](/architecture/await-boundaries)
- [Await Runtime Setup](/deploy/orchestrator-runtime/await)
- [Command Steps](/deploy/orchestrator-runtime/command)
- [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff)
- [State Model](/architecture/state-model)
