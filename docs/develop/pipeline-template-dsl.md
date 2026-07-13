# Pipeline template types and linear contracts

Version 2 pipeline templates support a compact type form and, for a strictly linear chain, optional logical contract propagation. These are authoring conveniences: they normalize into the same template model and do not change service, operator, mapper, or runtime execution behavior.

## Types

Use `types` for named pipeline types:

```yaml
types:
  PaymentRequest:
    fields:
      - [1, orderId, uuid]
      - [2, amount, decimal]
```

`messages` remains a supported compatibility alias, but emits a deprecation warning. A template must declare one alias, not both.

Field tuples are YAML arrays in the exact form `[number, name, type]`. Use them only when those are the complete field declaration. Object fields remain necessary for metadata such as `repeated`, `optional`, `reserved`, `overrides`, or `referenceable`.

```yaml
types:
  PaymentRequest:
    fields:
      - [1, orderId, uuid]
      - number: 2
        name: lineItems
        type: PaymentLineItem
        repeated: true
```

Tuple and object fields may be mixed in one type. They produce the same field model as the verbose form:

```yaml
messages: # compatibility alias; prefer types
  PaymentRequest:
    fields:
      - number: 1
        name: orderId
        type: uuid
```

## Linear logical contracts

Logical contracts name pipeline types. They are different from fully qualified Java implementation types on a step.

```yaml
contract:
  input: PaymentRequest
  output: PaymentOutcome

steps:
  - name: Validate Payment
    service: com.example.payment.ValidatePaymentService
    cardinality: ONE_TO_ONE
    output: ValidatedPayment

  - name: Process Payment
    service: com.example.payment.ProcessPaymentService
    cardinality: ONE_TO_ONE
    output: PaymentOutcome
```

`contract.input` enables propagation. The first step inherits it when its logical input is omitted. Each later step inherits the preceding resolved logical output when its input is omitted. An explicit logical step input remains valid and is checked as an assertion: it must equal the inherited contract.

`contract.output` is optional. When present, it asserts that the final step has that concrete logical output.

The `contract` block is intentionally separate from root `input` and `output`. Those root keys remain exclusively physical boundaries, so a subscription, object input, checkpoint, or object publication can coexist with logical propagation:

```yaml
input:
  subscription:
    publication: payment-requests
output:
  checkpoint:
    publication: payment-outcomes
contract:
  input: PaymentRequest
  output: PaymentOutcome
```

In v2 templates, step `input` and `output` always name logical pipeline contracts. When the compiling module can inspect a local Java service or operator, its signature and mapper resolution infer the Java execution types. Use the optional closed `java` block to assert that inferred binding, resolve an ambiguity, or declare the binding for a service that is outside the compiling module's classpath:

```yaml
- name: Process Payment
  service: com.example.payment.ProcessPaymentService
  input: PaymentRecord
  output: PaymentStatus
  java:
    input: com.example.domain.PaymentRecord
    output: com.example.domain.PaymentStatus
```

For remote and other coordinator-owned steps that have no inspectable local Java signature, `java` supplies the required coordinator-side binding. In a split-module pipeline, a local service hosted by another module is also non-inspectable from the current compilation, so its `java` bindings remain explicit. This is a build-topology constraint, not a different runtime execution model. A fully qualified v2 `input` or `output` remains accepted as deprecated compatibility syntax when paired with `inputTypeName` or `outputTypeName`; it emits a migration warning. Those `*TypeName` keys remain compatibility aliases for logical contracts.

### Mappers are separate from `java`

`java` identifies a Java domain type. A mapper performs an actual representation conversion, so it remains explicit at a conversion boundary:

| Boundary | Explicit declaration |
| --- | --- |
| Object ingest into the first business step | `input.emits.mapper` for the object snapshot and the first step's `inboundMapper` for the pipeline message/domain conversion |
| A service outside the current module | `inboundMapper` / `outboundMapper` when the generated cross-module client must translate the pipeline message and its domain type |
| Object publish from the terminal business step | terminal `outboundMapper` for the pipeline message/domain conversion, plus `output.consumes.mapper` to render the published object payload |

Do not add a mapper merely to restate an inspectable local service signature. Do add it when the boundary changes representation; signature inference does not replace an object adapter, a transport mapper, or a cross-module client mapper.

### Before and after

The following explicit v2 chain:

```yaml
messages:
  PaymentRequest:
    fields:
      - number: 1
        name: orderId
        type: uuid
  ValidatedPayment:
    fields:
      - number: 1
        name: orderId
        type: uuid
  PaymentOutcome:
    fields:
      - number: 1
        name: orderId
        type: uuid
steps:
  - name: Validate Payment
    cardinality: ONE_TO_ONE
    input: PaymentRequest
    output: ValidatedPayment
  - name: Process Payment
    cardinality: ONE_TO_ONE
    input: ValidatedPayment
    output: PaymentOutcome
```

The above chain can be authored as:

```yaml
types:
  PaymentRequest:
    fields: [[1, orderId, uuid]]
  ValidatedPayment:
    fields: [[1, orderId, uuid]]
  PaymentOutcome:
    fields: [[1, orderId, uuid]]
contract:
  input: PaymentRequest
  output: PaymentOutcome
steps:
  - name: Validate Payment
    cardinality: ONE_TO_ONE
    output: ValidatedPayment
  - name: Process Payment
    cardinality: ONE_TO_ONE
    output: PaymentOutcome
```

## Applicability

| Template shape | Compact `types` / tuples | Contract propagation |
| --- | --- | --- |
| Linear local pipeline | Yes | Yes, with a concrete named output at every predecessor |
| Delegated Java operator | Yes | Yes for logical contracts; Java signature inference is unchanged |
| Remote step | Yes | No; retain explicit logical `input`/`output` and required `java` bindings |
| Branch-aware pipeline (`accepts` or `terminal`) | Yes | No; keep explicit branch contracts |
| Physical input/output boundary | Yes | Yes, using the separate `contract` block |
| Union or non-singular predecessor output | Yes | No; declare the next input explicitly |
| Await, command, or query flow | Yes when otherwise valid | Keep explicit contracts unless the flow is demonstrably a supported linear chain |
| Version 1 template | No v2 alias migration | No |

Propagation stops when a predecessor has no concrete singular logical output. It also does not guess across branches or unions. A missing predecessor output, an inherited/explicit mismatch, or a final-output assertion mismatch is a compile-time error.

## Migration

1. In a v2 template, replace top-level `messages` with `types`.
2. Convert only field objects that contain exactly `number`, `name`, and `type`; preserve field order and all advanced object fields.
3. Do not declare both aliases.
4. For a demonstrably linear chain, add `contract.input` from the existing first logical input. Add `contract.output` only when the final logical output is concrete.
5. Remove a repeated logical step input only when it exactly equals the preceding logical output. Keep every output needed by the chain.
6. Move Java execution contracts into `java.input` and `java.output`; keep legacy fully qualified `input`/`output` plus `*TypeName` pairs only while migrating.

See the [pipeline compilation guide](./pipeline-compilation/) for build-time validation and generated artifacts.
