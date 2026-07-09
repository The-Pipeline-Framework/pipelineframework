---
search: false
---

# Typed Union Output Contracts

Typed union outputs add closed business outcomes to the TPF contract model without turning TPF into a general workflow engine.

## Design Boundary

TPF keeps the invariant that every step declares one output type. A union output is still one output type; it is a closed wrapper whose selected variant carries the concrete payload.

This differs from CNCF Serverless Workflow and AWS State Machine Language:

- CNCF Serverless Workflow uses `switch` with runtime `when` expressions over workflow data.
- AWS State Machine Language uses `Choice` states to route execution between states.
- TPF uses YAML contracts to generate typed Java, protobuf, REST, and mapper boundaries at build time.

The current slice keeps the pipeline linear and adds conservative type-based routing. There is still no graph DSL, arbitrary predicate language, fork/join runtime, or pipeline spawning.

## Compiler Ownership

The runtime template model owns the closed union declaration:

```yaml
unions:
  PaymentOutcome:
    variants:
      captured:
        type: PaymentCaptured
        number: 1
      rejected:
        type: PaymentRejected
        number: 2
```

Build-time validation guarantees:

- union names do not collide with top-level messages or built-in semantic types;
- each variant points at a known top-level message;
- variant names and protobuf field numbers are unique;
- message fields cannot reference union types.

IDL snapshots include unions so compatibility checks catch removed, renamed, renumbered, or retargeted variants.

## Transport Mapping

The generated protobuf shape is a wrapper message with `oneof`:

```proto
message PaymentOutcome {
  oneof outcome {
    PaymentCaptured captured = 1;
    PaymentRejected rejected = 2;
  }
}
```

REST and checkpoint JSON use a discriminated JSON object:

```json
{
  "type": "rejected",
  "orderId": "11111111-1111-1111-1111-111111111111",
  "failureCode": "PAYMENT_REJECTED"
}
```

Application code can use Jackson polymorphic annotations on a sealed interface to produce this shape until TPF generates sealed-type scaffolding. JSON-backed paths use that sealed domain type directly; they do not derive a `PaymentOutcomeDto` or require a protobuf-style union mapper.

For protobuf-backed paths, TPF generates a framework-owned union wrapper mapper such as `Mapper<PaymentOutcome, PipelineTypes.PaymentOutcome>`. That mapper only selects or unwraps the `oneof` variant and delegates field-level payload conversion to application mappers for the concrete variants, for example `Mapper<PaymentCaptured, PipelineTypes.PaymentCaptured>`.

`google.protobuf.Any` is intentionally not the default because it weakens the contract and hides exhaustiveness from the compiler.

## Linear Branch Routing

Branch routing now uses step applicability, not graph edges:

```yaml
steps:
  - name: Reserve Stock
    inputTypeName: OrderDecision
    outputTypeName: StockReserved
    accepts:
      - PhysicalOrder

  - name: Provision License
    inputTypeName: OrderDecision
    outputTypeName: LicenseProvisioned
    accepts:
      - DigitalOrder

  - name: Finalize
    inputTypeName: OrderCompletion
    outputTypeName: FinalizedOrder
    accepts:
      - StockReserved
      - LicenseProvisioned
    terminal: true
```

Compiler rules:

- `accepts` may reference concrete contract types only.
- union inputs with multiple concrete alternatives require explicit `accepts`.
- branch-aware pipelines are `ONE_TO_ONE` only in v1.
- there must be exactly one `terminal: true` step, and it must be last.
- the terminal step must cover every reachable branch-end alternative.

Runtime rules:

- the runner still walks the authored step list in order;
- steps whose accepted types do not match the current item are skipped as `not_applicable`;
- skips are emitted as replay events and do not mutate async checkpoint state;
- business steps never receive a union value just to no-op on the wrong variant.
