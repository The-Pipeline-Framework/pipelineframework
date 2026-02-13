# Using plugins

Plugins let you add cross-cutting capabilities (like persistence, metrics, or logging) without pushing that code into every business step.
You configure behavior through aspects; the framework wires and generates the integration pieces.

## Mental model

- **Steps**: your business transformations.
- **Aspects**: where and when a plugin should run.
- **Plugins**: implementation modules that provide the side effect.

This keeps business logic focused while infrastructure concerns stay declarative.

## What you configure

You decide:

- Which plugin aspects are enabled.
- Scope: `GLOBAL` or selected `STEPS`.
- Position: `BEFORE_STEP` or `AFTER_STEP`.
- Any plugin-specific parameters.

## What TPF handles for you

At build time and runtime, TPF handles:

- Adapter and client/server code generation.
- Transport-specific integration (gRPC/REST/LOCAL).
- Type-aware side-effect wiring.
- Runtime injection for generated plugin surfaces.

## Aspect naming and module mapping

Aspect names must be `lower-kebab-case` and map to the plugin module base name.
For example, aspect `persistence` maps to module `persistence-svc`.
This keeps dependency resolution deterministic.

## Side-effect transport contract

Side-effect plugins are modeled as unary services for the selected transport.
TPF generates type-indexed service contracts such as `ObservePaymentRecordSideEffectService` with shape `PaymentRecord -> PaymentRecord`, and inserts them at the configured aspect position.

## Build-time requirements

- A pipeline YAML config must be discoverable, so output types can be resolved for side-effect adapters.
  The loader checks module root and `config/` for `pipeline.yaml`, `pipeline-config.yaml`, or `*-canvas-config.yaml`.
- For gRPC transport, protobuf/descriptor content must include the required `Observe<T>SideEffectService` definitions.

## Plugin host modules

If you want plugin-server artifacts generated in a dedicated module, add a marker class annotated with `@PipelinePlugin("name")` in that module.
That scopes plugin-server generation there and avoids leaking plugin implementation dependencies into regular service modules.

## Example

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
