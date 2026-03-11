# Data Types

## Overview

Template IDL `version: 2` is semantic-first.

Authors declare the meaning of a field once, and the compiler derives:

- Java binding type
- protobuf wire type
- compatibility rules for schema evolution

Normal v2 authoring does **not** require `protoType`.

Legacy v1 templates that still declare `type` + `protoType` remain supported for backward compatibility, but v2 is the preferred model.

## Canonical Semantic Types

Supported built-ins in v2:

- `string`
- `bool`
- `int32`
- `int64`
- `float32`
- `float64`
- `decimal`
- `uuid`
- `timestamp`
- `datetime`
- `date`
- `duration`
- `bytes`
- `currency`
- `uri`
- `path`

Structural type:

- `map`

`map` is a parameterised collection type, not an atomic scalar. It must declare `keyType` and `valueType`.

Enums are not a first-class canonical v2 type in the current schema. Model them with named messages and string-backed fields for now, or keep legacy enum handling in v1 compatibility paths until explicit enum support is added.

PascalCase type tokens are treated as references to top-level named messages.

Example:

```yaml
messages:
  Money:
    fields:
      - number: 1
        name: amount
        type: decimal
      - number: 2
        name: currency
        type: currency
```

## Default Compiler Mappings

Default wire and Java bindings:

- `decimal` -> protobuf `string`, Java `BigDecimal`
- `uuid` -> protobuf `string`, Java `UUID`
- `timestamp` -> protobuf `string`, Java `Instant`
- `datetime` -> protobuf `string`, Java `LocalDateTime`
- `date` -> protobuf `string`, Java `LocalDate`
- `duration` -> protobuf `string`, Java `Duration`
- `currency` -> protobuf `string`, Java `Currency`
- `uri` -> protobuf `string`, Java `URI`
- `path` -> protobuf `string`, Java `Path`
- `bytes` -> protobuf `bytes`, Java `byte[]`

Important semantic distinctions:

- `timestamp`: absolute point in time
- `datetime`: local/civil date-time without timezone semantics
- `date`: calendar date only

`currency`, `uri`, and `path` stay first-class semantic types even though protobuf uses `string` by default.

## Collections and Messages

Use `repeated: true` for lists:

```yaml
- number: 3
  name: labels
  type: string
  repeated: true
```

Use `type: map` for maps:

```yaml
- number: 4
  name: metadata
  type: map
  keyType: string
  valueType: string
```

Use PascalCase message names for references:

```yaml
- number: 2
  name: money
  type: Money
```

## Reserved Fields and Compatibility

Named messages can declare reserved numbers and names:

```yaml
messages:
  ChargeResult:
    fields:
      - number: 1
        name: paymentId
        type: uuid
    reserved:
      numbers: [4, 5]
      names: ["legacyCode"]
```

The compiler emits a normalized IDL snapshot and can compare it against a baseline using `-Dtpf.idl.compat.baseline=<path>`.

Breaking changes fail compatibility checks when they:

- renumber fields
- change canonical field types
- change structural shape, such as adding `repeated: true` to a singular field, changing a map key or value type, switching a field to a different message reference, or changing between optional and required semantics
- remove fields without reserving the old number and name
- reuse reserved numbers or names

## Advanced Overrides

Overrides are optional and intended for exceptional cases only.

```yaml
- number: 2
  name: amount
  type: decimal
  overrides:
    proto:
      encoding: string
```

Overrides are validated. Unsafe or lossy encodings, such as mapping `decimal` to `double`, are rejected.

## Legacy v1 Note

v1 templates still allow:

```yaml
- name: amount
  type: BigDecimal
  protoType: string
```

That shape is legacy compatibility only. New templates should use semantic v2 fields and top-level `messages`.
