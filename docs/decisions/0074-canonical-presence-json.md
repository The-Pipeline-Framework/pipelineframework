---
title: Canonical field presence survives shared JSON boundaries
status: accepted
---

# ADR-0074: Canonical field presence survives shared JSON boundaries

## Context

Adding an optional payload reference to CSV Payments exposed a historical-input
regression in the control-plane transition decoder. An older JSON object omitted
the new field, but plain Jackson construction supplied Java null to a generated
record that correctly requires a non-null `CanonicalFieldValue` wrapper. Protobuf
compatibility alone did not exercise this JSON boundary.

[ADR-0042](0042-shared-canonical-pipeline-serialization.md) places shared JSON
policy in `pipelineframework-runtime-serialization` while allowing only the
semantic model as a TPF dependency. `CanonicalFieldValue` lives in the
framework-neutral `pipelineframework-runtime-core` artifact.

## Decision

Extend the serialization artifact's allowed TPF dependencies to include
`pipelineframework-runtime-core`, solely to serialize its shared runtime values.
The runtime-core model remains free of Jackson, Quarkus, CDI and host dependencies.
Runtime implementations, deployment artifacts and other framework dependencies
remain prohibited in the serialization artifact. This narrow extension was
approved by the human owner on 15 October 2026.

`PipelineJson` owns canonical field presence at JSON object boundaries:

- A missing field decodes to ABSENT and an ABSENT field is omitted on encoding.
- Explicit JSON null decodes to NULL and encodes as null. Generated non-nullable
  field validation still rejects this state.
- A present value encodes directly as that value and decodes using its declared
  generic type. False, empty collections and nested objects remain present.
- ABSENT at the root or in a position that cannot omit a field fails serialization;
  it must not silently become explicit null.

This policy does not change generated REST DTO mappings, which have their own
transport contract. It does not make a nullable field optional or relax generated
canonical constructors. No application-specific deserializer or altered admission
fixture may substitute for the shared policy.

## Consequences

Historical JSON inputs that omit newly optional fields remain readable. Focused
historical-input and round-trip tests cover absence, null, scalar values and
nested generic values; application compatibility must also exercise the actual
transition codec. Existing serialized fields retain their wire shape. A successful
local serialization test is not proof of distributed execution or release recovery.
