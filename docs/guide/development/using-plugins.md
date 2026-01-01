# How to use a plugin (Application Developer Guide)

Application developers use plugins by applying them to pipelines via aspects. Aspects are the primary abstraction for adding cross-cutting concerns like persistence, logging, or metrics to your pipeline.

## Mental model

Think of plugins as services you want to apply at specific points in your pipeline. Aspects are the configuration mechanism that tells the framework where and how to apply these plugins.

## What aspects are

Aspects are configuration elements that:
- Define where plugins apply in your pipeline
- Specify timing (before or after steps)
- Control ordering when multiple aspects apply
- Remain separate from your core pipeline logic

## Why aspects exist

Aspects separate cross-cutting concerns from your core business logic. This keeps your pipeline steps focused on business transformations while handling persistence, metrics, and other infrastructure concerns declaratively.

## How aspects differ from steps

- Steps transform data and are central to your business logic
- Aspects perform side effects and are infrastructure concerns
- Steps are always visible in your pipeline definition
- Aspects are applied during compilation and expand into internal steps

## Global vs step-scoped aspects

Currently, aspects support two scopes:

- **GLOBAL**: Applies to all steps in the pipeline
- **STEPS**: Reserved for future extensions (currently treated as GLOBAL with a warning)

## Positioning with BEFORE and AFTER

Aspects can be positioned:
- **BEFORE_STEP**: Executes before the main step
- **AFTER_STEP**: Executes after the main step

For example, you might use BEFORE_STEP for metrics collection at the start of processing and AFTER_STEP for persistence of results.

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
      "position": "AFTER_STEP",
      "order": 100
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
- The order of multiple plugins at the same position
- The configuration parameters for each plugin

## What is decided automatically

The framework handles:
- Transport protocols between pipeline and plugins
- Type conversion between domain and transport types
- Code generation for adapters
- Runtime injection of plugin implementations