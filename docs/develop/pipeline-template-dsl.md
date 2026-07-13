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

In v2 templates, step `input` and `output` always name logical pipeline contracts. Local Java service and operator signatures, including mapper resolution, continue to infer their Java execution types exactly as before. Use the optional closed `java` block only to assert an inferred binding or resolve an explicit Java ambiguity:

```yaml
- name: Process Payment
  service: com.example.payment.ProcessPaymentService
  input: PaymentRecord
  output: PaymentStatus
  java:
    input: com.example.domain.PaymentRecord
    output: com.example.domain.PaymentStatus
```

For remote and other coordinator-owned steps that have no inspectable local Java signature, `java` supplies the required coordinator-side binding. A fully qualified v2 `input` or `output` remains accepted as deprecated compatibility syntax when paired with `inputTypeName` or `outputTypeName`; it emits a migration warning. Those `*TypeName` keys remain compatibility aliases for logical contracts.

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
