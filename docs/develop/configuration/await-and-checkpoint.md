---
title: Await and Checkpoint Settings
---

# Await and Checkpoint Settings

Await and checkpoint configuration belongs to the runtime shell. It keeps human approvals, webhook callbacks, long-running provider responses, and cross-pipeline handoff out of business functions.

## Main Surfaces

| Surface | Purpose | Full reference |
| --- | --- | --- |
| Queue-async execution | durable background execution and retry ownership | [Orchestrator Background Execution](/develop/configuration/all-settings#orchestrator-background-execution) |
| Await transports | pending interaction and completion admission wiring | [Await Transports](/develop/configuration/all-settings#await-transports) |
| Checkpoint handoff bindings | publication targets and subscriber admission | [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff) |
| Function handler context | context attributes captured at function boundaries | [Function Transport Context Attributes](/develop/configuration/all-settings#function-transport-context-attributes-function-handlersadapters) |

## Related Guides

- [Await Boundaries](/design/await-boundaries)
- [Await Runtime Setup](/deploy/orchestrator-runtime/await)
- [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff)
- [State Model](/design/state-model)

