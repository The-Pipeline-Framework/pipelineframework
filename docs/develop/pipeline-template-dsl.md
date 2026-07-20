# Pipeline template DSL

Version 3 describes a pipeline in domain terms. A type says what business value is moving through the pipeline; a step says how that value changes. Protobuf tags, generated bindings, and transport representations are compiler-owned infrastructure.

Version 3 generates protobuf contracts and Java domain records from the same normalized type model. Java records and wrappers preserve nominal identity; aliases remain transparent. Generated sealed union APIs and their protobuf adapters preserve declared discriminator semantics.

```yaml
version: 3

types:
  OrderId:
    wraps: uuid

  Currency:
    wraps: string

  Description:
    alias: string

  Money:
    fields:
      - [amount, decimal]
      - [currency, Currency]

  PaymentApproved:
    fields:
      - [orderId, OrderId]
      - [authorizationId, string]

  PaymentDeclined:
    fields:
      - [orderId, OrderId]
      - [reason, string]

  PaymentRequiresReview:
    fields:
      - [orderId, OrderId]
      - [reason, string]

  PaymentOutcome:
    variants:
      approved: PaymentApproved
      declined: PaymentDeclined
      requiresReview: PaymentRequiresReview
```

## Domain types

Every entry in `types` declares exactly one kind of type.

| Declaration | Meaning |
| --- | --- |
| `fields` | A product type: this value contains all of these fields. |
| `wraps` | A nominal domain value over one semantic scalar. |
| `alias` | A transparent name for another type. |
| `variants` | A closed sum type: this value is one declared alternative. |

### Product types

Use `fields` for a named business record. A compact field is a YAML tuple in the exact form `[name, type]`; the object form is `{ name, type }`.

```yaml
types:
  PaymentRequest:
    fields:
      - [orderId, OrderId]
      - [amount, decimal]
      - name: lineItems
        type: PaymentLineItem
```

Field names are unique within a product type. A field can reference a semantic scalar or another named type.

### Nominal wrappers

Use `wraps` when the underlying representation has a distinct business identity. `OrderId` and `CustomerId` can both wrap `uuid` without becoming interchangeable.

```yaml
types:
  OrderId:
    wraps: uuid
  CustomerId:
    wraps: uuid
```

A wrapper is assignable only to the same wrapper. Conversion to or from the wrapped scalar is explicit at a generated or application-owned boundary; it is never an implicit substitution in a step contract.

Wrappers may declare target-neutral constraints beside `wraps`. Constraints are part of the semantic type model and are enforced by the generated Java wrapper constructor; they are not protobuf options, JSON Schema rules, or application validation annotations.

```yaml
types:
  CurrencyCode:
    wraps: string
    minLength: 3
    maxLength: 3
    pattern: "[A-Z]{3}"
  ContactEmail:
    wraps: string
    format: email
  PositiveAmount:
    wraps: decimal
    minimumExclusive: 0
```

String wrappers support `minLength`, `maxLength`, `pattern`, and `format: email`. Length is measured in Unicode code points. A `pattern` requires the entire string value to match. The DSL does not define Java regular expressions as its permanent pattern language: the Java target currently compiles the declared pattern with Java regex support, and another target must either support the pattern or report a clear limitation until TPF defines a portable profile. `format: email` checks a practical mailbox shape—one non-edge `@`, no whitespace, a non-empty local part, and non-empty dot-separated domain labels. It does not establish ownership, DNS validity, or deliverability.

Numeric wrappers (`int32`, `int64`, `float32`, `float64`, and `decimal`) support `minimum`, `minimumExclusive`, `maximum`, and `maximumExclusive`. Bounds are semantic decimal values.

Changing wrapper constraints is a semantic compatibility change. TPF classifies it as unchanged, narrowing, widening, or incomparable; this release rejects every change other than unchanged even though protobuf wire tags and shapes stay the same.

### Aliases

Use `alias` for a better name without creating a new nominal identity.

```yaml
types:
  Description:
    alias: string
  PaymentNarrative:
    alias: Description
```

An alias is assignable as its resolved target. Alias chains are allowed when they are acyclic.

### Discriminated unions

Use `variants` for a closed set of business outcomes. The variant key is the discriminator and is part of the domain contract; it does not derive from a payload class name.

```yaml
types:
  PaymentOutcome:
    variants:
      approved: PaymentApproved
      declined: PaymentDeclined
      requiresReview: PaymentRequiresReview
```

A union value is assignable to its union contract. A concrete variant can be introduced into that union, but the union itself is not assignable to a concrete variant. Variants reference named payload types; inline payload records and payload-less variants are intentionally outside this DSL.

A union declares a contract, not a routing graph. Branch applicability remains type-based and linear. Use a union contract where a step consumes the complete outcome set; use `accepts` only when a branch deliberately narrows that set.

When a branch-aware step declares a union as its `input`, omitting `accepts` means it accepts every declared variant. An explicit list narrows the accepted payload contracts and must be a subset of that union. The final `terminal: true` step must cover every alternative still reachable after earlier branches; the compiler reports uncovered alternatives before generation.

```yaml
steps:
  - name: Classify payment
    input: PaymentRequest
    output: PaymentOutcome

  - name: Handle approval
    input: PaymentOutcome
    accepts: [PaymentApproved]
    output: PaymentResult

  - name: Complete payment
    input: PaymentOutcome
    terminal: true
    output: PaymentResult
```

TPF records the union name, declared discriminator, and payload contract in generated branching, replay, and checkpoint-handoff metadata. This makes an observed alternative identifiable without exposing protobuf field numbers. Routing remains payload-type based: if two declared variants intentionally share a payload type, they route together under `accepts`; discriminators are not accepted as routing predicates.

## Wire identity and compatibility

Names, field names, and variant discriminators are the DSL-facing identities. The compiler allocates protobuf tags and records them in the sibling IDL lock file (`pipeline.idl.json` for `pipeline.yaml`). YAML never contains field or variant numbers.

The compiler preserves tags and reservations as types evolve. Changing a field representation, a wrapper representation, an alias target, or a variant discriminator changes the contract and is checked before generation. A generated target must preserve nominal identity and discriminator semantics; a target that cannot do so reports a clear diagnostic instead of silently flattening the type.

### Generated protobuf contracts

Version 3 emits a shared `pipeline-types.proto`. Records become protobuf messages, and authored camel-case field names are rendered as deterministic `snake_case` protobuf fields. Each eligible singular scalar has proto3 explicit presence; named messages and `payload_ref` already have message presence.

A wrapper is a distinct message with `value = 1`, so two wrappers over the same scalar remain distinct on the wire. An alias emits no message and resolves transitively to its target protobuf type. A union becomes a message with `oneof value`; each discriminator becomes a `snake_case` oneof field, while the authored discriminator remains the semantic identity in `pipeline.idl.json`.

The generator reserves removed protobuf names and tags from the committed IDL state. Source declaration order does not affect retained or newly allocated tags.

### Generated Java domain types

Run `PipelineV3ContractGenerator` in the same `generate-sources` lifecycle as protobuf generation. It invokes the independent protobuf and Java target generators from the same resolved v3 type model and committed IDL state.

Generated Java sources live under `<basePackage>.domain`. A record field keeps its YAML declaration order in the generated Java record constructor. A wrapper is a distinct one-component record, so two wrappers over the same scalar cannot be exchanged accidentally. Aliases generate no class and use their resolved target type.

The generated `PipelineDomainProtoAdapters` class converts generated records, wrappers, and unions to and from the generated protobuf types. It is public application-facing generated code, but its exact class and method shape remains provisional while the Java target continues to evolve.

Generated record scalar components are nullable boxed/reference types. `null` means that an eligible proto3 scalar was absent; a present scalar default remains its Java default value. This preserves transport presence only. It does not define required fields, business validity, or refinement rules. A wrapper is different: `Currency(null)` is invalid, while a `null` wrapper field in a containing record represents absence. For constrained wrappers, the generated compact constructor is the Java invariant boundary. `payload_ref` is handled separately as the framework `PayloadReference` contract type.

Each v3 union generates a sealed Java interface. Its nested variant records carry the declared payload and expose the exact YAML discriminator through `discriminator()`. The adapter maps those variants directly to the generated protobuf `oneof` cases; it does not flatten them into their payloads.

For unary local services whose Java signature uses those exact generated domain records, wrappers, or unions, TPF uses the generated adapters directly. Application-owned Java types remain representation boundaries and continue to need the normal explicit mapper path. Branch-aware v3 templates keep the existing `accepts` and `terminal` model; a union remains a closed contract rather than a predicate or graph language. Remote/framework-owned steps and non-unary v3 execution remain pending.

## Pipeline contracts

Logical contracts use the names declared in `types`. They are distinct from Java implementation types.

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

`contract.input` supplies the first omitted step input. In a linear chain, each later omitted input inherits the preceding concrete output. An explicit input is an assertion and must agree with the inherited contract. `contract.output`, when present, asserts the final concrete output.

Physical boundaries remain under root `input` and `output`, so they can coexist with logical contracts:

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

Propagation never guesses across a union, a branch, or a predecessor without one concrete output. Those contracts stay explicit.

## Java bindings and mappers

Step `input` and `output` always name logical pipeline contracts. For an inspectable local service or operator, TPF infers Java execution types from the signature and resolved mappers. Use `java` to assert that inference, resolve an ambiguity, or supply the coordinator-side binding when the service is outside the compiling module.

```yaml
- name: Process Payment
  service: com.example.payment.ProcessPaymentService
  input: PaymentRecord
  output: PaymentOutcome
  java:
    input: com.example.domain.PaymentRecord
    output: com.example.domain.PaymentOutcome
```

For a remote or framework-owned step without an inspectable local Java contract, `java` provides the required coordinator-side binding.

::: tip Compilation visibility is topology-scoped
Java-type and mapper discovery runs in the annotation-processing compilation unit currently being built. It sees only services and mappers on that module's compile classpath; sibling modules in the same repository are not automatically visible.

The runtime mapping chooses generated roles and logical placement. The Maven build topology chooses the classpath for each generated role. Evaluate discovery independently for every compiling module; provide `java.input` / `java.output` and the required mapper whenever that module cannot inspect the service or representation boundary. See [Runtime layouts and build topologies](/deploy/runtime-layouts/).
:::

A Java binding identifies a domain type. A mapper performs a representation conversion and remains explicit at a real conversion boundary.

| Boundary | Required declaration |
| --- | --- |
| Object ingest into the first business step | `input.emits.mapper` for the object snapshot and the first step's `inboundMapper` for the pipeline/domain conversion |
| Service outside the compiling module | `java.input` / `java.output`, plus `inboundMapper` / `outboundMapper` when the generated client crosses representations |
| Object publish from the terminal business step | terminal `outboundMapper` and `output.consumes.mapper` |

Do not add a mapper only to restate an inspectable local service signature. Add one whenever the boundary changes representation.

## Deliberately small language

The DSL does not provide inline union payloads, payload-less variants, optional shorthand, generic type expressions, recursive types, units of measure, arbitrary smart constructors, predicate routing, or workflow-graph semantics. Keep business-state modeling explicit through named product types, wrappers, aliases, and closed unions.

See the [pipeline compilation guide](./pipeline-compilation/) for build-time validation and generated artifacts.
