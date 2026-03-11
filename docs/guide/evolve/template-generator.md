# Template Generator (Reference)

This guide explains how to use the Pipeline Framework template generator to create complete pipeline applications from YAML configuration files.

<Callout type="tip" title="Canvas Is the Default">
Use the visual Canvas designer at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a> for day-to-day creation. The template generator is a secondary option for automation or CI-only workflows.
</Callout>

## Overview

The template generator creates a complete Maven multi-module pipeline project from a template config. It generates:

- parent and module POMs
- common domain/DTO/mapper code
- service modules for each pipeline step
- the orchestrator module
- runtime config and test scaffolding

The `sample-config` command emits **IDL v2** sample configs by default (internal: `generateSampleConfig`), while user-provided configs passed through `--config` are treated as v1 when `version` is not explicitly set (internal: `loadConfig`).

## Schema Reference

Use the source JSON schema for automation:

- <https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/template-generator-node/src/pipeline-template-schema.json>

## v2 Template Shape

v2 keeps `platform` and `transport` as top-level concerns and makes field typing semantic-first.

Key points:

- top-level `messages:` are the preferred contract model
- steps still use `inputTypeName` / `outputTypeName`
- fields declare semantic `type`, not `protoType`, in normal cases
- stable field numbers are required in v2

Example:

```yaml
version: 2
appName: "Payment Processing Pipeline"
basePackage: "com.example.payments"
transport: "GRPC"

messages:
  PaymentInput:
    fields:
      - number: 1
        name: paymentId
        type: uuid
      - number: 2
        name: amount
        type: decimal
      - number: 3
        name: processedAt
        type: timestamp

  PaymentOutput:
    fields:
      - number: 1
        name: paymentId
        type: uuid
      - number: 2
        name: status
        type: string
    reserved:
      numbers: [4]
      names: ["legacyCode"]

steps:
  - name: "Process Payment"
    cardinality: "ONE_TO_ONE"
    inputTypeName: "PaymentInput"
    outputTypeName: "PaymentOutput"
```

## Generating a Sample Config

Generate a sample config:

```bash
node template-generator-node/bin/generate.js sample-config
```

This produces a v2 sample config with top-level `messages`.

Use the Node.js `sample-config` command when you want a lightweight v2 config scaffold. Use the Java JAR when you need the full runnable Maven and Java project structure.

## Generating an Application

Generate the complete application from your config:

```bash
java -jar template-generator-1.0.0.jar --config my-pipeline-config.yaml --output ./my-pipeline-app
```

The generator copies the authored config into `config/pipeline.yaml` so the build can recreate `.proto` definitions during `generate-sources`.

## Semantic Types and Derived Bindings

In v2, the author normally chooses only the semantic type:

```yaml
- number: 2
  name: amount
  type: decimal
```

The compiler derives the bindings:

- protobuf `string`
- Java `BigDecimal`

This avoids lossy combinations like `BigDecimal` + `double`.

Common semantic types:

- `decimal`
- `uuid`
- `timestamp`
- `datetime`
- `date`
- `duration`
- `currency`
- `uri`
- `path` (filesystem path semantics, not a web/resource identifier)
- `bytes`
- `map`

Map fields use `keyType` and `valueType` to define the entry contract:

```yaml
- number: 4
  name: metadata
  type: map
  keyType: string
  valueType: string
```

PascalCase type tokens are treated as named message references.

```yaml
- number: 5
  name: paymentDetails
  type: PaymentInput # PaymentInput is a referenced message type
```

## Advanced Overrides

Overrides are available, but they are an advanced escape hatch:

```yaml
- number: 2
  name: amount
  type: decimal
  overrides:
    proto:
      encoding: string
```

Unsafe overrides are rejected during loading/normalization. That includes lossy type changes, overrides that break canonical invariants, unsupported encodings, and overrides that conflict with message or map structure. For example, `decimal` with `overrides.proto.encoding: double` is rejected, while `decimal` with `overrides.proto.encoding: string` remains valid.

## Compatibility Checking

The runtime emits a normalized IDL snapshot at build time and can compare it against a baseline:

```bash
./mvnw -Dtpf.idl.compat.baseline=/path/to/main-idl.json test
```

Compatibility checks fail on breaking changes such as:

- field renumbering
- canonical type changes
- message-reference or map-structure changes
- removing a field without reserving its old number and name

## Legacy v1 Templates

Legacy templates that still use inline `inputFields` / `outputFields` with authored `protoType` remain supported.

They are treated as backward-compatible input, not the preferred authoring model for new templates.
