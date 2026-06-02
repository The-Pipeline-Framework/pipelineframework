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

The first slice is contract support only. There is no automatic branch dispatch, `switch`, fork/join, or pipeline spawning.

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

## Future Branching

Future workflow routing can build on the same union contract:

```yaml
choice:
  from: Capture Payment
  on:
    captured: Dispatch Order
    rejected: Compensate Order
```

That routing layer should fail the build when a union variant is not handled, unless the author declares an explicit default branch. Until then, application code handles the sealed interface through normal polymorphic domain behavior in the next step.
