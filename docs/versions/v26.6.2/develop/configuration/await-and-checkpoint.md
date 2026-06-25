---
title: Await, Command, and Checkpoint Settings
search: false
---

# Await, Command, and Checkpoint Settings

Await, command, and checkpoint configuration belongs to the runtime shell. It keeps human approvals, webhook callbacks, long-running provider responses, replay-safe external effects, and cross-pipeline handoff out of business functions.

## Main Surfaces

| Surface | Purpose | Full reference |
| --- | --- | --- |
| Queue-async execution | durable background execution and retry ownership | [Orchestrator Background Execution](/versions/v26.6.2/develop/configuration/all-settings#orchestrator-background-execution) |
| Await transports | pending interaction and completion admission wiring | [Await Transports](/versions/v26.6.2/develop/configuration/all-settings#await-transports) |
| Command connectors | connector endpoint, credentials, timeout, provider retry tuning, and effect-store implementation | [Command Steps](/versions/v26.6.2/deploy/orchestrator-runtime/command) |
| Checkpoint handoff bindings | publication targets and subscriber admission | [Checkpoint Handoff](/versions/v26.6.2/deploy/orchestrator-runtime/checkpoint-handoff) |
| Function handler context | context attributes captured at function boundaries | [Function Transport Context Attributes](/versions/v26.6.2/develop/configuration/all-settings#function-transport-context-attributes-function-handlersadapters) |

## Related Guides

- [Await Boundaries](/versions/v26.6.2/design/await-boundaries)
- [Await Runtime Setup](/versions/v26.6.2/deploy/orchestrator-runtime/await)
- [Command Steps](/versions/v26.6.2/deploy/orchestrator-runtime/command)
- [Checkpoint Handoff](/versions/v26.6.2/deploy/orchestrator-runtime/checkpoint-handoff)
- [State Model](/versions/v26.6.2/design/state-model)
