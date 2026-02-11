# Data Types

## Overview

This page is a reference for the Java type names used in pipeline templates and how they map to protobuf types for gRPC. It reflects what the **template generator** understands and what the **proto generator** emits. It is not a runtime restriction.

If you are hand-authoring templates or need special mappings, use the `protoType` field on a template entry and provide your own MapStruct converters.

## Source of Truth

### Template generator (type list + UI defaults)
- `template-generator-node/src/pipeline-template-schema.json` defines the allowed type names in the generator UI/schema.
- `template-generator-node/src/handlebars-template-engine.js` and `template-generator-node/src/browser-template-engine.js` map those types to proto scalars for generated scaffolding and imports.
- `template-generator-node/templates/common-converters.hbs` is optional scaffold for MapStruct conversions.

### Runtime/compiler (proto generation + binding validation)
- `framework/runtime/src/main/java/org/pipelineframework/proto/PipelineProtoGenerator.java` generates `.proto` files from pipeline templates and resolves list/map types.
- `framework/deployment/src/main/java/org/pipelineframework/processor/extractor/PipelineStepIRExtractor.java` and `framework/deployment/src/main/java/org/pipelineframework/processor/ir/TypeMapping.java` capture domain/mapper bindings.
- `framework/deployment/src/main/java/org/pipelineframework/processor/util/GrpcBindingResolver.java` validates bindings against compiled protobuf descriptors.

Note: these paths may move during refactors. If a path changes, search the repository for the class name and follow the current source of truth.

In short: the generator defines the **type menu**; the proto generator emits the `.proto` shapes; the compiler validates **bindings**; MapStruct (and your converters) handle **actual conversions**.

## Default Type Mappings (Generator)

### Basic Types
- String -> string
- Integer -> int32
- Long -> int64
- Double -> double
- Float -> float
- Boolean -> bool
- Byte -> int32
- Short -> int32
- Character -> string

### Binary Types
- byte[] -> bytes

### Rich Java Types (mapped to string unless noted)
- UUID -> string
- BigDecimal -> string
- BigInteger -> string
- Currency -> string
- Path -> string
- URI -> string
- URL -> string
- File -> string

### Date/Time Types
- LocalDateTime -> string
- LocalDate -> string
- OffsetDateTime -> string
- ZonedDateTime -> string
- Instant -> string
- Duration -> string
- Period -> string

### Atomic Types
- AtomicInteger -> int32
- AtomicLong -> int64

### Collection Types
- `List<T>` -> repeated T (inner type must be a valid protobuf scalar or message name)

### Map Types
- `Map<K, V>` -> map<key, value> (for example `Map<String, String>` -> `map<string, string>`)
  - Keys are mapped to protobuf scalars.
  - Values are mapped to protobuf scalars or left as message types.

### Enums and Optional
- Enum -> protobuf enum (or string if you choose to map as text).
- `Optional<T>` -> represent presence using proto3 optional/oneof or wrapper types, and map to nullable DTO fields.

## Conversions

The framework does **not** provide a universal converter library. The generator can scaffold a `CommonConverters` class, but it is optional and expected to be customized per project. MapStruct mappers are responsible for conversions between domain/DTO/gRPC types.

If you override `protoType` in your template, ensure your mappers handle the conversion correctly.

## Example Template

```yaml
steps:
  - name: "ProcessPayment"
    inputTypeName: "PaymentInput"
    inputFields:
      - name: "paymentId"
        type: "UUID"
      - name: "amount"
        type: "BigDecimal"
      - name: "processedAt"
        type: "LocalDateTime"
      - name: "labels"
        type: "List<String>"
      - name: "metadata"
        type: "Map<String, String>"
```
