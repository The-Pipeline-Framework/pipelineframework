---
title: Native runtimes consume compiler-owned execution contracts
status: draft
---

# ADR-0080: Native runtimes consume compiler-owned execution contracts

## Context

Native LOCAL workflow equivalence is a product requirement for polyglot TPF applications.
An external business-function service cannot run a whole native pipeline locally merely
by receiving generated RPC stubs. The imperative shell must execute in the same process
as the local business functions. The earlier Quarkus-only executor proposal therefore
does not satisfy the requirement.

This draft records the architectural boundary proposed to satisfy that requirement.
It does not select detailed execution-plan or wire syntax, claim existing Node/Python
support, or schedule implementation. The assessment and epic track delivery scope.

## Decision

The compiler remains the single authority for v3 normalisation, type and wire identity,
composition, validation and placement. Language-specific runtimes consume compiler-owned
executable contracts; they do not independently interpret the authored DSL.

Each runtime owns local sequencing and the supported invocation policy shell, including
context, failure/retry ownership, cancellation and semantic observations. Business functions
return to that shell, which invokes the next operation. LOCAL means invocation in the same
process. Compiler-generated remote bindings connect different placements without changing
business functions or semantic identity.

The existing Java coordinator remains the durable authority. Native workers execute an
admitted range and return outcomes through shared coordinator contracts. They do not gain
coordinator-store access or independently decide admission, durable retries, DLQ or recovery.
Synchronous native execution does not require a coordinator hop.

Existing Quarkus Connector/provider implementations remain in their current host. Native
execution does not imply portable provider authoring. Any supported external capability
binding must preserve authorisation, capture/effect identity and outcome semantics.
Unsupported target capabilities are rejected explicitly rather than omitted at runtime.

## Boundary qualification before implementation

Contracts #52 must define a closed, versioned execution profile, a responsibility map for
each A-return-to-B-invocation operation and enabled hook, and shared conformance/negative
tests. Name decision, execution, state and retry owners explicitly. Unsupported capabilities
fail compilation or artifact admission; adapters cannot silently add execution policy.

The bounded research probe may inform this contract. Production runtime implementation
depends on acceptance of it. Completion is measured against the declared profile and layouts,
not against accidental dependencies discovered during implementation. Semantic expansion
requires a shared-contract revision and affected-language conformance review. If the boundary
cannot be isolated, reconsider the profile rather than importing coordinator/provider authority.

The assessment retains the earlier LOCAL-first release candidate as historical reasoning,
not current release guidance. Hosted Query and native Provider SPI remain separate unselected
options; none changes existing authority or approves provider authoring, Cloud integration
or implementation scheduling.

[ADR-0088](./0088-polyglot-contract-qualification-precedes-runtime-selection.md) refines this
qualification gate with the adopted contracts #52 research profile and ownership map. Its unary
fixture is a research probe, not an accepted product MVP: streaming, Connectors and durable
Await remain requirements. Native kernels, the Java-daemon boundary and Detroit have not been
selected for production by that probe. The subsequent bounded Node evaluation applies
the qualified contract through compiler-generated bindings and an opt-in native package.
This experimental selection does not authorise broader production support. Delivery
scope and continue/redesign/retire gates remain with the epic.

## Rationale

One compiler-owned semantic model can support multiple conforming execution implementations.
This preserves native LOCAL execution without making a remote service facade equivalent
to a monolith or duplicating durable coordination. Spring can reuse Java contracts and
implementation; Node and Python require native kernels with the same conformance evidence.

## Consequences

- Contracts and compiler own portable data, executable projections and compatibility rules;
  runtime owns kernels and coordinator integration; examples/system-tests own executable
  cross-language and cross-artifact evidence.
- Every claimed execution profile needs conformance for each declared layout and execution mode.
  A LOCAL-only experimental profile does not claim distributed or durable support; those remain
  requirements of the broader roadmap.
  A bounded first profile does not constitute full feature parity or remove existing Java support.
- Payload/failure compatibility, policy resolution, feature qualification and native release
  packaging must be settled before implementation claims. These are tracked by contracts #52.
- Maintaining native kernels is an explicit cost; no separate coordinator or Provider SDK is
  justified solely by native LOCAL support.
- No implementation or code deletion is authorised by this draft.

See the [architecture assessment](/evolve/polyglot-applications-assessment),
[epic #1055](https://github.com/The-Pipeline-Framework/pipelineframework/issues/1055),
[ADR-0002](./0002-compiler-owned-v3-type-model.md) and
[ADR-0050](./0050-remote-worker-protocols-are-shared-runtime-contracts.md).
