---
search: false
---

# How to use a plugin (Application Developer Guide)

Application developers use plugins by applying them to pipelines via aspects. Aspects are the primary abstraction for adding cross-cutting concerns like persistence, logging, or metrics to your pipeline.

## Mental model

Think of plugins as services you want to apply at specific points in your pipeline. Aspects are the configuration mechanism that tells the framework where and how to apply these plugins.

## What aspects are

Aspects are configuration elements that:
- Define where plugins apply in your pipeline
- Specify timing (before or after steps)
- Remain separate from your core pipeline logic

## Why aspects exist

Aspects separate cross-cutting concerns from your core business logic. This keeps your pipeline steps focused on business transformations while handling persistence, metrics, and other infrastructure concerns declaratively.

## How aspects differ from steps

- Steps transform data and are central to your business logic
- Aspects perform side effects and are infrastructure concerns
- Steps are always visible in your pipeline definition
- Aspects are applied during compilation and expand into internal steps

## Aspect naming and module mapping

Aspect names must be lower-kebab-case and match the plugin module base name. For example, an aspect named
`persistence` is expected to be implemented by a module named `persistence-svc`. This naming is used to wire
plugin dependencies consistently and avoid ambiguous module resolution.

## Side-effect transport contract

Side-effect plugins observe stream elements and are exposed as unary services for the configured transport (gRPC or REST).
The framework generates deterministic type-indexed services like `ObservePaymentRecordSideEffectService` with the
signature `PaymentRecord -> PaymentRecord`, and injects them into the stream after each step.

## Build-time requirements

- A pipeline config YAML must be available so the processor can discover step output types for type-indexed side-effect adapters.
  The loader searches the parent module root and a `config/` subfolder for `pipeline.yaml`, `pipeline-config.yaml`,
  or `*-canvas-config.yaml`.
- For gRPC transport, protobuf definitions must include the type-indexed `Observe<T>SideEffectService` services for any
  observed type, and the descriptor set must include those definitions.

## Plugin host modules

To generate plugin-server artifacts in a dedicated module, add a marker class annotated with `@PipelinePlugin("name")`
inside that module. This tells the annotation processor to emit plugin-server adapters and producers only there, so
regular service modules do not depend on plugin implementations.

## Global vs step-scoped aspects

Aspects support two scopes:

- **GLOBAL**: Applies to all steps in the pipeline
- **STEPS**: Applies to the configured `targetSteps` list in the aspect config

## Positioning with BEFORE and AFTER

Aspects can be positioned:
- **BEFORE_STEP**: Executes before the main step
- **AFTER_STEP**: Executes after the main step

BEFORE_STEP and AFTER_STEP are applied as configured during compilation.

## Example: Pipeline with persistence aspect

Without aspect:
```json
{
  "steps": [
    {
      "name": "ProcessOrder",
      "cardinality": "ONE_TO_ONE",
      "inputTypeName": "Order",
      "outputTypeName": "ProcessedOrder"
    }
  ]
}
```

With persistence aspect:
```json
{
  "steps": [
    {
      "name": "ProcessOrder",
      "cardinality": "ONE_TO_ONE",
      "inputTypeName": "Order",
      "outputTypeName": "ProcessedOrder"
    }
  ],
  "aspects": {
    "persistence": {
      "enabled": true,
      "scope": "GLOBAL",
      "position": "AFTER_STEP"
    }
  }
}
```

## Deployment options

Plugins can be deployed in different ways:
- **Embedded**: Run in the same process as the pipeline
- **Shared runtime**: Multiple pipelines share plugin instances
- **Separate service**: Plugin runs as an independent service

The choice affects performance, scaling, and operational complexity but doesn't change how you configure aspects.

## What application developers control

You decide:
- Which plugins to apply
- Where in the pipeline to apply them (BEFORE/AFTER)
- The configuration parameters for each plugin

## What is decided automatically

The framework handles:
- Transport protocols between pipeline and plugins
- Type conversion between domain and transport types
- Code generation for adapters
- Runtime injection of plugin implementations
