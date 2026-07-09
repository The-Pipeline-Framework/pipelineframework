---
title: Configuration
search: false
---

# Configuration

TPF configuration is split by when the setting is read and what boundary it affects.

Use this guide to choose the right configuration surface first. Use [All Settings](/versions/v26.7.1/develop/configuration/all-settings) when you need the full key inventory.

## Reading Path

| Need | Start here |
| --- | --- |
| Generated artifacts, annotation processor options, REST path overrides | [Build-Time Settings](/versions/v26.7.1/develop/configuration/build-time) |
| Runtime execution, clients, telemetry, health, in-flight probe | [Runtime Settings](/versions/v26.7.1/develop/configuration/runtime) |
| Await, command steps, queue-async, checkpoint handoff, background execution | [Await, Command, and Checkpoint Settings](/versions/v26.7.1/develop/configuration/await-and-checkpoint) |
| REST, gRPC, LOCAL transport modes; `FUNCTION` platform and generated entry points | [Transport and Platform Settings](/versions/v26.7.1/develop/configuration/transport-and-platform) |
| Persistence, caching, materialization, reject sinks | [Providers and Plugins](/versions/v26.7.1/develop/configuration/providers-and-plugins) |
| Replay Viewer URL/query parameter behavior | [Replay Viewer Parameters](/versions/v26.7.1/develop/configuration/replay-viewer-parameters) |
| Lambda-specific configuration slice | [Lambda-Focused Configuration](/versions/v26.7.1/develop/configuration/lambda-focused) |

## Configuration Rule

Keep application decisions in YAML and Java contracts. Use runtime properties for deployment choices, provider selection, endpoint locations, retry budgets, and observability settings.

If changing a setting would alter the generated code shape, it belongs in build-time configuration. If changing it only changes how a generated runtime connects, stores, observes, or schedules work, it belongs in runtime configuration.
