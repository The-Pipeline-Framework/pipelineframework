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

A named type normally maps to the Java type generated under the application's `basePackage`. An
independently distributed definition can instead supply the canonical Java class with `java`:

```yaml
types:
  DocumentFile:
    java: org.pipelineframework.blocks.document.DocumentFile
    fields: [[sourceId, string], [fileName, string], [contentType, string], [content, payload_ref]]
```

This is still one canonical v3 type. The compiler validates its fields and uses the bound class at
ordinary Java and representation-provider boundaries; it does not treat the class as an untyped
payload or create a parallel package type system.

### Contributed protocol types

Version 3 can reference framework- or extension-owned protocol vocabulary without copying its declaration into the application:

```yaml
types:
  Decision:
    variants:
      call: <acme.tools.ProtocolCall>
      complete: Recommendation
```

`<namespace.TypeName>` is a build-time type reference. The compiler resolves it from the framework catalog or static Connector Provider artifact metadata, imports that immutable definition into the normalized v3 type model, and then applies the same union, assignability, generation, adapter, serialization, and release-contract rules as an application-authored type. It is not a generic type expression or a dynamic payload escape.

A short reference such as `<ProtocolCall>` is accepted only when exactly one registered contribution has that type name. Use the qualified identity in durable source when multiple providers publish the same short name. Unknown references, ambiguous short names, duplicate identities, and collisions with application-authored names fail compilation.

Only referenced contributions and their dependencies enter the application contract. Installing an unused provider does not change generated domain types or release identity. Provider metadata encodes the same record, wrapper, alias, and union shapes documented on this page; it is not an external schema-import language. Provider construction, configuration, connections, credentials, and execution are not involved in type resolution.

### Product types

Use `fields` for a named business record. A compact singular field is a YAML tuple in the exact form `[name, type]`; the singular object form is `{ name, type }`. Use `{ name, repeated: type }` for an ordered, finite, zero-or-more field that preserves duplicates.

```yaml
types:
  PaymentRequest:
    fields:
      - [orderId, OrderId]
      - [amount, decimal]
      - name: lineItems
        repeated: PaymentLineItem
```

Field names are unique within a product type. A singular or repeated field can reference a semantic scalar or another named type. `type` and `repeated` are mutually exclusive; `repeated: true` is not valid v3 syntax.

For singular fields, presence and value nullability are independent. A `?` after the field name
controls whether the key may be absent; a `?` after the type controls whether an explicitly present
value may be null:

```yaml
types:
  Customer:
    fields:
      - [name, string]
      - [nickname?, string]
      - [middleName, string?]
      - [note?, string?]
```

| Declaration | Presence | Value | Canonical JSON states |
| --- | --- | --- | --- |
| `[name, string]` | required | non-null | value only |
| `[nickname?, string]` | optional | non-null | absent or value |
| `[middleName, string?]` | required | nullable | null or value; key required |
| `[note?, string?]` | optional | nullable | absent, null, or value |

The verbose equivalent uses explicit normalized terms:

```yaml
- name: nickname
  type: string
  presence: optional
  nullability: non_null
```

Do not combine compact `?` markers with verbose semantic properties on the same field. Compact and
verbose forms normalize identically. The normalized IDL and hashed canonical contract contain
`REQUIRED`/`OPTIONAL` and `NON_NULL`/`NULLABLE`; authored punctuation does not survive normalization.
An unmarked field keeps the existing strongest semantics: required presence and a non-null value.

Generated Java keeps unmarked required/non-null fields as `T`. Any other singular state uses
`CanonicalFieldValue<T>`, whose `Absent`, `NullValue`, and `Value` cases preserve the distinction.
Generated constructors enforce the declared state. Nullable protobuf fields use a compiler-owned
`oneof` with a value arm and a `google.protobuf.NullValue` arm; no selected arm means absent.
Compiler-owned tags and names are persisted in `pipeline.idl.json` and reserved when retired.

Defaults are not part of this feature. In particular, an absent field is not silently materialized
as a default value. Repeated fields retain their existing missing-to-empty-list behavior and cannot
currently declare presence or nullability modifiers.

Compatibility diagnostics name their surface. For example, adding a required field is safe for the
protobuf wire because it adds a tag, but it breaks canonical data produced by the previous contract
because those payloads lack the new required key. Changing required to optional or non-null to
nullable widens canonical input while changing generated Java APIs; the reverse transitions break
existing canonical payloads.

For the matrix below, `RNN`, `ONN`, `RN`, and `ON` mean required/non-null,
optional/non-null, required/nullable, and optional/nullable. Rows are the previous contract and
columns are the current contract. This is backward canonical-data compatibility: can every payload
accepted by the previous contract still be accepted?

| Previous \ Current | RNN | ONN | RN | ON |
| --- | --- | --- | --- | --- |
| RNN | unchanged | widening | widening | widening |
| ONN | breaking: absent | unchanged | breaking: absent; null widened | widening |
| RN | breaking: null | breaking: null; absence widened | unchanged | widening |
| ON | breaking: absent/null | breaking: null | breaking: absent | unchanged |

The normalized-IDL classification follows the same accepted-value relationship. Protobuf presence
changes retain the same field tag and are wire compatible. Non-null to nullable retains the value tag
and adds a null-marker tag, so it is wire compatible. Nullable to non-null is wire-readable but lossy:
old explicit-null marker values become an unselected value field. Every state transition changes the
generated Java/domain contract under the conservative source-compatibility policy; transitions to or
from `RNN` also change between `T` and `CanonicalFieldValue<T>`.

Other record evolution is classified separately:

| Change | Protobuf wire | Canonical data | Generated Java/domain |
| --- | --- | --- | --- |
| add required singular field | compatible | breaking | breaking constructor/component surface |
| add optional singular field | compatible | compatible | breaking constructor/component surface |
| remove field with reserved tags/names | compatible for readers | breaking for closed record JSON | breaking |
| singular ↔ repeated | lossy | breaking scalar/array shape | breaking `T`/`List<T>` shape |
| change type | depends on protobuf wire type; review required | breaking | breaking |

`repeated` describes multiplicity inside one domain value. Generated Java records expose it as an immutable, non-null `List<T>`; generated protobuf uses a `repeated` field; and generated JSON Schema uses an array whose missing value normalizes to an empty array. Order and duplicates are preserved. Adding a repeated field is therefore compatible with an older pinned IDL snapshot and with protobuf/JSON readers: old payloads observe the field as empty. Changing an existing field between singular and repeated is incompatible.

Repeated fields do not imply reactive cardinality. Expanding a `List<T>` into a `Multi<T>` or collecting a `Multi<T>` into a list is application logic and must be explicit in the step implementation, for example:

```java
public Multi<PaymentLineItem> process(PaymentRequest request) {
    return Multi.createFrom().iterable(request.lineItems());
}

public Uni<PaymentRequest> process(Multi<PaymentLineItem> items) {
    return items.collect().asList()
        .map(collected -> new PaymentRequest(orderId, amount, collected));
}
```

The corresponding steps declare `ONE_TO_MANY` and `MANY_TO_ONE`. TPF does not insert a collection-to-stream or stream-to-collection conversion.

### External representations

A v3 domain type can optionally declare a named external representation for a framework component. The pipeline and business functions continue to use the generated canonical domain type; the component performs the explicit conversion at its boundary.

```yaml
types:
  PaymentOutput:
    fields:
      - [id, uuid]
      - [amount, decimal]
    mappings:
      persistence:
        type: org.example.persistence.PaymentOutputEntity
        mapper: org.example.persistence.PaymentOutputPersistenceMapper
```

`mappings` is a generic named-type declaration. A mapping key is not limited to a predefined spelling by the DSL, and a mapping can omit either class name for a future representation resolver. Consumers define their own readiness rules. In this release, the `persistence` consumer requires both values, supports concrete record domain types, and requires the declared mapper to implement `Mapper<GeneratedDomain, Representation>`.

The mapper's `toExternal` direction is used only when writing the persistence representation. Its `fromExternal` direction remains part of the same generic mapper contract for readers and later boundaries; a persistence write does not round-trip the saved entity back into the pipeline.

The `file` representation is mapper-free. It applies to a record containing one `payload_ref` field
and declares `type: java.nio.file.Path` on both sides of an ordinary step. Input options bound the
materialized size. Output options name an Object Publish target, bound the published size, and may
override the v1 filename-derived object key. The generated facade preserves the step's normal
`ONE_TO_ONE` or `ONE_TO_MANY` cardinality.

At the service boundary, the mapping changes what the author implements:

| Step cardinality | Authored service |
| --- | --- |
| `ONE_TO_ONE` | `ReactiveService<Path, Path>` returning `Uni<Path>` |
| `ONE_TO_MANY` | `ReactiveStreamingService<Path, Path>` returning `Multi<Path>` |

The YAML step still names the canonical input and output contracts:

```yaml
steps:
  - name: Render pages
    service: com.example.documents.RenderPagesService
    cardinality: ONE_TO_MANY
    input: SourceDocument
    output: RenderedPage
```

For Object Ingest, `selection.mode: together` can construct one canonical input from several listed
objects. Use `selection.keys` for differently named fields, or `selection.into` for one repeated
`payload_ref` field. TPF generates the projection in both cases; the first authored service receives
the canonical record directly. Complete configuration and Java examples are in
[Object Ingest And Publish](../design/object-ingest.md#grouped-selection).

### Preview representation support

Version 3 representation support is experimental and intentionally narrow. Generated protobuf adapters are the normal transport boundary for generated v3 domain values. The `persistence` consumer supports an explicit mapping for a generated record when both the representation and `Mapper<GeneratedDomain, Representation>` are available to the compiling module. CSV Payments also proves the same generic mapper contract at an OpenCSV row boundary before the first canonical business step. The `file` consumer is the mapper-free payload-reference boundary for ordinary `Path` services.

JSON, REST, checkpoint, object-publish, and broker boundaries retain their current application-owned or transport-owned conversion paths. They do not yet infer or generate a `json` representation from `mappings`. A declared mapping is therefore not a user-selectable conversion mode and does not promise support from every component.

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

String wrappers support `minLength`, `maxLength`, `pattern`, and `format: email`. Length is measured in Unicode code points. A `pattern` requires the entire string value to match and must be paired with `maxLength`; generated Java validates that bound before attempting the match, limiting pattern processing for boundary data. The DSL does not define Java regular expressions as its permanent pattern language: the Java target currently compiles the declared pattern with Java regex support, and another target must either support the pattern or report a clear limitation until TPF defines a portable profile. `format: email` checks a practical mailbox shape—one non-edge `@`, no whitespace, a non-empty local part, and non-empty dot-separated domain labels. It does not establish ownership, DNS validity, or deliverability.

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

A union value is assignable to its union contract. A concrete variant can be introduced into that union. When a downstream step declares a concrete variant as its input, the compiler routes only that variant from an earlier union-producing step; this does not make the union contract itself substitutable for every concrete variant. Variants reference named payload types, including contributed `<Type>` references; inline payload records and payload-less variants are intentionally outside this DSL.

A union declares a contract, not a routing graph. Branch applicability remains type-based and linear. Use a union contract where a step consumes the complete outcome set; use `accepts` only when a branch deliberately narrows that set.

When a branch-aware step declares a union as its `input`, omitting `accepts` means it accepts every declared variant. An explicit list narrows the accepted payload contracts and must be a subset of that union. The final `terminal: true` step must cover every alternative still reachable after earlier branches; the compiler reports uncovered alternatives before generation.

A step can also declare a concrete variant directly when it only handles that outcome. This is equivalent to a type-based branch from the preceding union; use `accepts` when the step keeps the union contract as its input and wants to name the accepted alternatives explicitly.

```yaml
steps:
  - name: Classify payment
    input: PaymentRequest
    output: PaymentOutcome

  - name: Process declined payment
    input: PaymentDeclined
    output: PaymentResult
```

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

### Await projector typing with unions

A v3 Await step uses the same branch plan as every other step. When `accepts` names one
union alternative, the compiler narrows the generated Await request and projector input
to that alternative:

```yaml
- name: Clarify
  kind: await
  cardinality: ONE_TO_ONE
  input: PreparationDecision
  accepts: [ClarificationRequired]
  output: Prepared
  timeout: PT8H
  await:
    correlation: { strategy: interactionId }
    completion:
      type: com.example.ClarificationAnswer
      projector: com.example.ClarificationProjector
    transport: { type: interaction-api }
```

`ClarificationProjector` must implement
`AwaitCompletionProjector<ClarificationRequired, ClarificationAnswer, Prepared>`.
For multiple accepted alternatives, or all alternatives when `accepts` is omitted, its
input type remains `PreparationDecision`. The projector must be a public concrete class
with a public no-argument constructor. Raw, wildcard, unresolved, or incompatible
generic arguments fail compilation.

When `accepts` selects only some union alternatives, the omitted alternatives do not
invoke the Await step. They bypass it and continue unchanged through the ordinary v3
pipeline flow.

The `java` block is optional for a v3 Await boundary because these Java types are
inferred from the compiler-owned semantic model and projector. If supplied, both
`java.input` and `java.output` are required and must agree with that inference. A Java
binding never requests an implicit conversion.

## Wire identity and compatibility

Names, field names, and variant discriminators are the DSL-facing identities. The compiler allocates protobuf tags and records them in the sibling IDL lock file (`pipeline.idl.json` for `pipeline.yaml`). YAML never contains field or variant numbers.

Representation mappings are application/build configuration, not domain wire identity. Changing a declared representation class or mapper does not change protobuf tags or automatically constitute a v3 compatibility change.

The compiler preserves tags and reservations as types evolve. Changing a field representation, a wrapper representation, an alias target, or a variant discriminator changes the contract and is checked before generation. A generated target must preserve nominal identity and discriminator semantics; a target that cannot do so reports a clear diagnostic instead of silently flattening the type.

### Generated protobuf contracts

Version 3 emits a shared `pipeline-types.proto`. Records become protobuf messages, and authored camel-case field names are rendered as deterministic `snake_case` protobuf fields. Each eligible singular scalar has proto3 explicit presence; named messages and `payload_ref` already have message presence.

A wrapper is a distinct message with `value = 1`, so two wrappers over the same scalar remain distinct on the wire. An alias emits no message and resolves transitively to its target protobuf type. A union becomes a message with `oneof value`; each discriminator becomes a `snake_case` oneof field, while the authored discriminator remains the semantic identity in `pipeline.idl.json`.

The generator reserves removed protobuf names and tags from the committed IDL state. Source declaration order does not affect retained or newly allocated tags.

### Generated Java domain types

Run `PipelineContractGenerator` in the same `generate-sources` lifecycle as protobuf generation. It invokes the independent protobuf and Java target generators from the same resolved v3 type model and committed IDL state.

Generated Java sources live under `<basePackage>.domain`. A record field keeps its YAML declaration order in the generated Java record constructor. A wrapper is a distinct one-component record, so two wrappers over the same scalar cannot be exchanged accidentally. Aliases generate no class and use their resolved target type.

The generated `PipelineDomainProtoAdapters` class converts generated records, wrappers, and unions to and from the generated protobuf types. It is public application-facing generated code, but its exact class and method shape remains provisional while the Java target continues to evolve.

Unmarked required/non-null record components use `T` and reject Java null. Every optional or nullable singular component uses `CanonicalFieldValue<T>`: `Absent` means no canonical key, `NullValue` means an explicitly present null, and `Value` carries `T`. The generated compact constructor enforces the declared state, independently of protobuf presence. Repeated components retain their separate list semantics: their generated compact constructor normalizes Java null to `List.of()` and defensively copies supplied values with `List.copyOf(...)`. A wrapper such as `Currency(null)` remains invalid. For constrained wrappers, the generated compact constructor is the Java invariant boundary. `payload_ref` is handled separately as the framework `PayloadReference` contract type.

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

### Local and packaged pipeline composition

`pipelines` declares compile-time local definitions. Installed block artifacts can contribute the
same definition shape from an ordinary Maven or Gradle dependency. A `pipeline` step invokes either
source as an ordinary typed step; the compiler links the definition into the root release contract,
and the runtime returns its result to the caller without creating another execution or publishing
another terminal output.

```xml
<dependency>
  <groupId>org.pipelineframework.blocks</groupId>
  <artifactId>document-text-extraction</artifactId>
  <version>1.0.0</version>
</dependency>
```

```yaml
steps:
  - name: Extract document
    pipeline: document-text-extraction
    input: DocumentFile
    output: ExtractedDocument
```

Package definitions have qualified identities such as
`org.pipelineframework.document/document-text-extraction`. In an application, the short name is accepted only
when it is unique and does not collide with a local definition. Inside a block package, a short reference
resolves to that package's own definition; use a qualified identity for an explicit cross-package reference.
Removing the dependency makes the reference fail at compilation. Schema v3 release metadata records the
artifact version, definition resource, and definition fingerprint; changing them changes the consuming
contract hash.

Packaged definitions may contain authored service steps, nested `pipeline:` calls, and
operation-first Query or Command steps. A Block declares the external capabilities required by each
definition in its schema 1 package manifest; it does not declare application connector bindings:

```json
{
  "schemaVersion": 1,
  "namespace": "org.pipelineframework.graphql",
  "artifact": {
    "groupId": "org.pipelineframework.blocks",
    "artifactId": "graphql",
    "version": "1.0.0"
  },
  "definitions": [{
    "name": "graphql-mutation",
    "resource": "META-INF/pipeline/graphql.yaml",
    "requires": {
      "graphql.write": { "kind": "COMMAND" }
    }
  }]
}
```

Inside that definition, `using` names the requirement and the remaining fields are ordinary v3
operation-first fields:

```yaml
- name: Execute mutation
  kind: command
  using: graphql.write
  operation: execute.mutation
  operationVersion: 1
  input: <tpf.graphql.GraphQlMutationRequest>
  output: <tpf.graphql.GraphQlResponse>
```

The application grants the capability at compilation, once per qualified Block definition:

```yaml
connectors:
  primary-graphql:
    provider: graphql.smallrye
    version: 1
    config: { connection: primary-graphql }

blockBindings:
  org.pipelineframework.graphql/graphql-mutation:
    graphql.write:
      using: primary-graphql
      commandIdGenerator: com.example.GraphQlMutationCommandId
      duplicatePolicy: RETURN_RECORDED
      policy:
        requiredExecutionPosture: AUTOMATED
        minimumMachineConfirmation: PROVIDER_ACKNOWLEDGED
```

`blockBindings` is compile-time input: it is removed before ordinary v3 parsing and does not become
runtime configuration. Query mappings contain only `using`. Command mappings must explicitly select
`commandIdGenerator`, `duplicatePolicy`, and `policy`; a Block is forbidden from supplying those
authority choices. The selected provider operation must match the Block step's kind, provider and
operation major versions, cardinality, and canonical input/output Java contracts exactly.

Blocks still cannot package connector bindings, legacy `query:` or `command:` declarations, inline
connectors, Await, checkpoint handoff, remote Operators/delegates, callables, or dynamic-operation
steps. Application bindings retain endpoint, credentials, tenant/account selection, authorization,
and Command policy. Imported Query and Command steps normalize to the same descriptors and runtime
support used by local steps; there is no Block execution subsystem.

The [GraphQL Connector and Blocks](./extension/graphql-connector.md) proof applies this split to a
persisted Query and Mutation catalogue while leaving documents, connections, tenant selection, and
Command policy in the consuming application.

### Bounded recursion

```yaml
pipelines:
  process-attempt:
    input: AttemptState
    output: AttemptResult
    steps:
      - name: Decide
        service: com.example.DecideService
        input: AttemptState
        output: Decision
      - name: Continue
        pipeline: process-attempt
        input: Decision
        output: AttemptResult
        accepts: [ContinueDecision]
      - name: Complete
        service: com.example.CompleteService
        input: Decision
        output: AttemptResult
        accepts: [CompleteDecision]
      - name: Return
        service: com.example.ReturnService
        input: AttemptResult
        output: AttemptResult
        terminal: true

steps:
  - name: Run attempt
    pipeline: process-attempt
    input: AttemptState
    output: AttemptResult
    terminal: true
```

Direct self-recursion is a structured nested invocation, not a jump to an earlier step. Each invocation continues forward through its own ordered steps, and ordinary union routing plus `accepts` supplies the base case. Recursive definitions currently require an aggregate `ONE_TO_ONE` contract. Mutual recursion, recursive streaming cardinalities, and Await inside a nested definition are not supported.

The runtime bounds recursive depth with `pipeline.max-recursive-depth` (default `64`). A value of `0` allows composition but rejects the first recursive call. Reaching the configured depth is valid; attempting the next call fails immediately with a non-retryable `PipelineRecursionLimitExceededException`.

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

For a remote or framework-owned step without an inspectable local Java contract, `java` provides the required coordinator-side binding. V3 Await is the exception: its generated boundary is inferred from its semantic input/output types, `accepts`, and typed completion projector as described above.

::: tip Compilation visibility is topology-scoped
Java-type and mapper discovery runs in the annotation-processing compilation unit currently being built. It sees only services and mappers on that module's compile classpath; sibling modules in the same repository are not automatically visible.

The runtime mapping chooses generated roles and logical placement. The Maven build topology chooses the classpath for each generated role. Evaluate discovery independently for every compiling module; provide `java.input` / `java.output` and the required mapper whenever that module cannot inspect the service or representation boundary. See [Runtime layouts and build topologies](/deploy/runtime-layouts/).
:::

A Java binding identifies a domain type. A mapper performs a representation conversion and remains explicit at a real conversion boundary.

| Boundary | Required declaration |
| --- | --- |
| Object ingest into the first business step | `input.emits.mapper` for non-grouped object ingest; grouped `selection.mode: together` drives `selection`-based projection and does not need an `emits.mapper` |
| Service outside the compiling module | `java.input` / `java.output`, plus `inboundMapper` / `outboundMapper` when the generated client crosses representations |
| Object publish from the terminal business step | terminal `outboundMapper` and `output.consumes.mapper` |

Do not add a mapper only to restate an inspectable local service signature. Add one whenever the boundary changes representation.

## Deliberately small language

The DSL does not provide inline union payloads, payload-less variants, optional shorthand, generic type expressions, recursive types, units of measure, arbitrary smart constructors, predicate routing, or workflow-graph semantics. Keep business-state modeling explicit through named product types, wrappers, aliases, and closed unions.

See the [pipeline compilation guide](./pipeline-compilation/) for build-time validation and generated artifacts.
