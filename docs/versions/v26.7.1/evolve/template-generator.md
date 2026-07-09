---
search: false
---

# Template Generator (Reference)

::: tip Repository Boundary
The generator-facing schema is exported from this repository, but the MCP bridge and Node template generator live in [`tpf-mcp-bridge`](https://github.com/The-Pipeline-Framework/tpf-mcp-bridge). For the current user-facing entry point, see [MCP and Template Generation](/versions/v26.7.1/develop/mcp-template-generation).
:::

This guide explains how to use the Pipeline Framework template generator to create complete pipeline applications from YAML configuration files.

<Callout type="tip" title="Canvas Is the Default">
Use the visual Canvas designer at <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">https://app.pipelineframework.org</a> for day-to-day creation. The template generator is a secondary option for automation or CI-only workflows.
</Callout>

## Overview

The template generator creates a complete Maven multi-module pipeline project from a template config. It generates:

- parent and module POMs
- common domain/DTO/mapper code
- service modules for each pipeline step
- the orchestrator module
- runtime config and test scaffolding

The generator source now lives in the separate [`tpf-mcp-bridge`](https://github.com/The-Pipeline-Framework/tpf-mcp-bridge) repository. The source in this repository no longer carries a local `template-generator-node` checkout.

The generator-facing schema authority remains in this repository: `framework/deployment` packages `META-INF/pipeline/pipeline-template-schema.json` in the deployment artifact. The bridge repo vendors that generated schema for package/runtime use and refreshes it from a built framework artifact.

## Schema Reference

Use the exported JSON schema for automation:

- `framework/deployment/target/classes/META-INF/pipeline/pipeline-template-schema.json`
- <https://github.com/The-Pipeline-Framework/tpf-mcp-bridge/blob/main/template-generator-node/src/pipeline-template-schema.json>

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

Use the generator snapshot in the `tpf-mcp-bridge` repository when you need a sample config or direct generator development:

```bash
git clone https://github.com/The-Pipeline-Framework/tpf-mcp-bridge.git
cd tpf-mcp-bridge
```

For normal application scaffolding, prefer the MCP bridge workflow and its `generate_scaffold` tool. The bridge owns the vendored generator snapshot used for scaffold creation.

## Generating an Application

Generate the complete application from your config:

```bash
java -jar template-generator-<version>.jar --config my-pipeline-config.yaml --output ./my-pipeline-app
```

Replace `<version>` with the installed template-generator version.
You can determine that version with `template-generator --version`, by checking the project's release tags or changelog, or by looking up the published artifact on Maven Central or the project's releases page.

The generator copies the authored config into `config/pipeline.yaml` so the build can recreate `.proto` definitions during `generate-sources`.

Await steps are part of the v2 template schema so CI and automation can validate authored `kind: await` pipeline configs. The generator scaffolding surface lives in the `tpf-mcp-bridge` repository and must keep runtime support separate from generated dependency wiring. Runtime supports `interaction-api`, `webhook`, Kafka await, and SQS await; scaffold support for Kafka and SQS must emit the matching Quarkus dependencies, channel or poller properties, and queue/topic configuration rather than assuming generic await wiring is enough.

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

Unsafe overrides are rejected during loading and normalization.

- Lossy type changes are rejected.
  Example: changing a field from `int64` to `int32` is rejected because values may no longer fit.
- Overrides that break canonical invariants are rejected.
  Example: renaming a canonical field or changing the stable field ordering contract through overrides is rejected.
- Unsupported encodings are rejected.
  Example: setting `overrides.proto.encoding: uuid` on a `decimal` field is rejected because that encoding is not valid for the base type.
- Overrides that conflict with message or map structure are rejected.
  Example: changing a scalar field into a map, or reshaping a map key into a repeated field via overrides, is rejected.

For example, `decimal` with `overrides.proto.encoding: double` is rejected. `decimal` with `overrides.proto.encoding: string` remains valid.

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
