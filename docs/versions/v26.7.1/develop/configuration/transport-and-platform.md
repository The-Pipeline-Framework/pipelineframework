---
title: Transport and Platform Settings
search: false
---

# Transport and Platform Settings

Transport controls how generated components call each other. Platform controls whether generated entry points run as a service/runtime shape or through function-style handlers.

## Current Terms

| Term | Meaning |
| --- | --- |
| `GRPC` | generated gRPC server/client adapters |
| `REST` | generated REST resource/client adapters |
| `LOCAL` | generated in-process clients without a remote hop |
| `COMPUTE` | service/resource runtime path |
| `FUNCTION` | function-handler runtime path |

Transport and platform are related, but they are not the same dimension. For example, HTTP/Lambda-style execution is modeled as `pipeline.platform=FUNCTION` with REST-oriented generated handlers.

## Full Reference

- [Pipeline YAML](/versions/v26.7.1/develop/configuration/all-settings#pipeline-yaml)
- [REST Path Overrides](/versions/v26.7.1/develop/configuration/all-settings#rest-path-overrides-build-time)
- [REST Client Endpoints](/versions/v26.7.1/develop/configuration/all-settings#rest-client-endpoints)
- [Function Transport Context Attributes](/versions/v26.7.1/develop/configuration/all-settings#function-transport-context-attributes-function-handlersadapters)
- [Lambda-Focused Configuration](/versions/v26.7.1/develop/configuration/lambda-focused)
