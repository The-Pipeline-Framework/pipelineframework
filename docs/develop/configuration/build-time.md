---
title: Build-Time Settings
---

# Build-Time Settings

Build-time settings affect generated code, validation, or artifact shape. They are read during compilation or template generation.

## Main Surfaces

| Surface | Examples | Full reference |
| --- | --- | --- |
| Pipeline YAML | `transport`, `platform`, step declarations, mapper declarations | [Pipeline YAML](/develop/configuration/all-settings#pipeline-yaml) |
| Orchestrator annotation | generated CLI name, description, version | [Orchestrator CLI](/develop/configuration/all-settings#orchestrator-cli-annotation) |
| Annotation processor options | provider class, generated transport controls, validation flags | [Annotation Processor Options](/develop/configuration/all-settings#annotation-processor-options) |
| REST path overrides | generated REST resource path shape | [REST Path Overrides](/develop/configuration/all-settings#rest-path-overrides-build-time) |
| Build validation | contract, mapper, transport, plugin, and per-step validation | [Build-Time Validation](/develop/configuration/all-settings#build-time-validation-annotation-processor) |

## Design Guidance

Use build-time settings when the compiler needs to understand the contract before deployment:

- input and output types,
- step cardinality,
- mapper compatibility,
- transport compatibility,
- generated adapter shape,
- provider availability required for generation.

Avoid using runtime properties to compensate for a mismatched generated model. If a step contract, mapper, or transport shape is wrong, fix the YAML or Java contract and rebuild.
