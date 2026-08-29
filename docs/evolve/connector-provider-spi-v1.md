# Connector Provider SPI v1 — Pinned M1 Contract

Status: **Pinned architecture contract for M1**

This document reconciles issues #566–#570 and is the source of truth for implementation issues #571 onward.

## Core rule

**Unify provider mechanics, not operation semantics.**

TPF will provide a common Connector Provider ecosystem for identity, discovery, lifecycle, typed configuration, connections/secrets, runtime services, diagnostics, testing and tooling. Command, Query, Agent and object-streaming families retain distinct execution semantics.

## Public async rule

The public host-neutral Connector Provider API uses JDK async types.

- Unary asynchronous provider operations use `CompletionStage<T>`.
- The new public SPI must not expose Mutiny `Uni` or `Multi`.
- Quarkus/runtime adapters may use Mutiny internally.
- Existing legacy APIs may remain Mutiny-based behind compatibility adapters.
- Object streaming remains family-specific. Finite Query streaming is pinned separately through
  host-neutral `StreamingQueryOperation`/`QueryStream` contracts rather than exposing `Multi` to providers.

A universal `CompletionStage<O> execute(...)` is explicitly rejected. `CompletionStage` is the shared async mechanism, not the shared semantic contract.

## Provider model

Conceptually:

```text
ConnectorProvider<PC>
    ├── CommandOperation<I,OC,O>
    ├── QueryOperation<I,OC,O>
    ├── StreamingQueryOperation<I,OC,O>
    ├── ObjectSourceOperation     // specialised list/read semantics
    └── ObjectTargetOperation     // specialised write-session semantics
```

### ConnectorProvider

A provider is the reusable packaging/lifecycle unit. It owns provider identity, provider-level configuration, shared resources and an operation catalog.

The public SPI belongs in `framework/runtime-core` under `org.pipelineframework.connector` and subpackages. It must have no CDI, Quarkus or Mutiny dependency.

Provider lifecycle uses a provider-lifetime `ConnectorRuntimeContext` and JDK async completion.

### ConnectorOperation

`ConnectorOperation` exposes only stable operation identity and version. Its family is derived from
the executable subinterface; authors do not repeat the family in a descriptor.

Operation identity is pinned by provider ID, operation ID, operation kind and major operation version. Duplicate identities fail deterministically.

V1 recognises Command, Query, Object Source and Object Target semantics. Agent remains deferred and
has no public marker type until an executable contract is pinned.

## Reconciliation decision 1 — typed outcomes are part of the public operation contract

Command and Query operations return typed semantic outcomes, not bare values.

Conceptually:

```java
interface CommandOperation<I, C, O> extends ConnectorOperation {
    CompletionStage<CommandOutcome<O>> dispatch(CommandInvocation<I, C> invocation);
}

interface QueryOperation<I, C, O> extends ConnectorOperation {
    CompletionStage<QueryOutcome<O>> query(QueryInvocation<I, C, O> invocation);
}
```

Exact names may change during implementation only for mechanical consistency; the semantic shape is pinned.

## Reconciliation decision 2 — provider runtime context and invocation context are separate lifetimes

`ConnectorRuntimeContext` is provider-lifetime infrastructure only. It may expose approved executors/runtime services, connection resolution, secret resolution, telemetry and clock/runtime identity.

It must not contain mutable "current execution" state.

Execution-scoped information belongs to operation invocation context carried by `CommandInvocation` / `QueryInvocation`, including tenant, execution, step, release, correlation/trace and deadline information as applicable.

## Reconciliation decision 3 — provider config, operation config and invocation input are distinct

TPF distinguishes three kinds of data:

```text
Provider configuration
    lifecycle/resource/connection configuration

Operation configuration
    static pipeline configuration for one operation

Invocation input
    dynamic business/domain data
```

A provider may therefore have provider-level typed config `PC`, while an operation has operation-level typed config `OC`.

The exact generic arrangement may be simplified in implementation, but these three semantic scopes must not be collapsed into one arbitrary map.

Provider-defined configuration uses immutable typed Java records and generated/derived schema metadata. New provider-facing code must not require manual `Map<String,Object>` casts.

A marker interface such as `ConnectorConfig` is **not required** unless implementation proves it enforces a real invariant.

## Reconciliation decision 4 — execution mechanics are structural

Execution behavior that changes framework invocation is expressed by operation interfaces:

```java
interface BlockingOperation extends ConnectorOperation {}
interface BlockingQueryOperation<I, C, O> extends QueryOperation<I, C, O>, BlockingOperation {}
interface BlockingCommandOperation<I, C, O> extends CommandOperation<I, C, O>, BlockingOperation {}
interface SerializedOperation extends ConnectorOperation {}
```

Ordinary operations are invoked directly and may own their asynchronous execution. Blocking
operations are invoked on a framework worker and their returned stage is flattened. Serialized
operations admit one provider stage at a time for each configured binding and operation; the gate
covers the full stage lifetime.

Provider manifests do not repeat these facts as capability metadata. Parameterized metadata is
appropriate only where a runtime or compiler must validate a value that cannot be represented by a
specialization, and only when TPF implements the corresponding enforcement.

### Command capabilities

`CommandCapabilities` owns effect-specific semantics:

- attended vs automated execution posture;
- retry/redrive support;
- provider idempotency support;
- reconciliation support;
- confirmation/evidence strength;
- command-specific concurrency requirements where not expressible generically.

### Query capabilities

`QueryCapabilities` owns observation-specific semantics:

- cacheability;
- freshness/max age;
- consistency;
- repeatability/capture behaviour;
- timeout;
- pagination;
- streaming cardinality is structural (`StreamingQueryOperation`), not a Query capability flag.

Operation-family capabilities must not become a union of execution mechanics and every Command or
Query semantic axis.

## Reconciliation decision 5 — concurrency claims follow enforcement

TPF currently enforces only structural per-binding, per-operation serialization. General numeric,
provider-wide, and connection-wide limits are intentionally absent until a concrete provider needs
them and the runtime can enforce them. Provider-managed limits remain ordinary provider behavior.

## Reconciliation decision 6 — Agent remains deferred

M1 reserves the architectural possibility of Agent without publishing an empty operation kind or marker interface.

Agent may share provider mechanics:

- identity/versioning;
- discovery/registry;
- lifecycle;
- typed configuration;
- connection/secret references;
- runtime services;
- common provider test infrastructure.

M1 does **not** define:

- agent invocation/result execution API;
- models/model selection;
- memory/context lifecycle;
- planning;
- prompts;
- tools/skills/tool loops;
- tool authority;
- human approval semantics;
- deterministic replay;
- durable agent audit model;
- whether agents orchestrate/query/command other connector operations.

The current LLM Query connector contributes the portable, inert `AgentCall` data type for
application-authored decision unions. That proposal payload is not an `AgentOperation`, does not execute
anything, and does not change this SPI's decision to keep Agent execution semantics out of the universal
provider mechanics.

No generated Agent step or runtime execution path is added in M1/M2 unless a later pinned design explicitly introduces one.

## Provider identity, discovery and lifecycle

Provider IDs are stable lowercase dotted names. Exact-major compatibility is pinned; minor evolution is additive only.

Plain-Java discovery supports explicit provider collections and direct `ServiceLoader<ConnectorProvider>` discovery.
Provider construction is a host-internal concern, not an author contract. Plain Java uses the public
no-argument packaging constructor; Quarkus creates non-contextual CDI instances through the discovered
bean so injection, post-construction and destruction remain container-owned.

Native provider packaging derives descriptors from executable provider instances and emits direct
service registration plus `META-INF/pipeline/connector-providers.json`. Manifest schema v2 may also
bundle immutable protocol type descriptors under the provider's own namespace. Those descriptors
encode exactly the canonical v3 record, wrapper, alias, and union shapes; they are compile-time
vocabulary, not operations or an alternative external schema language. The consuming compiler reads
and validates the metadata without instantiating providers or resolving connections/secrets. Schema v1
manifests remain valid and contribute no protocol types.

Quarkus/CDI integration is an adapter. The public SPI never calls `CDI.current()` and provider authors must not need Quarkus build-step knowledge.

Discovery instances are catalog inputs, not lifecycle/resource owners. Each configured binding lazily
creates one binding-owned provider instance on first live use; operations using that binding share it,
while separate bindings receive separate instances. Replay-only paths do not create or start providers.
Shutdown rejects new activations, waits for in-flight activation, and stops every activated binding once
in reverse binding order.

Discovery still returns provider instances today because direct `ServiceLoader` and deprecated
provider-first compatibility are instance-based. Removing these prototypes entirely in favor of generated
metadata plus implementation classes is a follow-up; until then they remain strictly non-lifecycle catalog objects.

## Connector registry

`ConnectorRegistry` is the runtime catalog/validation boundary.

It must support deterministic validation and lookup for:

- duplicate provider IDs;
- duplicate operation identities;
- incompatible major versions;
- malformed descriptors;
- unsupported operation kinds at execution time;
- capability/policy incompatibility as those semantics are implemented.

The registry must be constructible without CDI for plain-Java tests and external-provider conformance.

## Authorized callable-operation snapshots

Registration and discovery establish the available operation universe; they do not grant model authority. An external authority supplies an immutable set of already-authorized `ConnectorOperationIdentity` values. `CallableOperationSnapshotProjector` resolves only those identities from `ConnectorProviderManifestCatalog`, the same framework-generated provider and operation metadata consumed by compiler/runtime validation, and produces a deterministic `CallableOperationSnapshot` sorted by stable identity.

The framework derives normalized input/output contracts from the typed `QueryOperation` and `CommandOperation` declarations when it generates provider artifacts; operation authors do not supply a second descriptor. The projection contains operation identity/kind/version, a stable description derived from that identity, the generated type contract, Query cache semantics and only model-relevant Command posture/confirmation semantics. It excludes provider and operation configuration schemas, retry/idempotency/reconciliation mechanics, durable-reference mechanics, provider instances, connection/secret references, resolved credentials, sessions and runtime handles.

The snapshot identity is a versioned SHA-256 digest over a canonical encoding of its model-visible contents. Registry order, authorization-set order and provider implementation technology do not affect it. A recursive caller retains the same immutable snapshot; refresh policy, authorization policy, operation dispatch, LLM integration and MCP import are separate concerns.

## Command contract

A Command represents an external effect.

### Identity

`CommandId` is stable logical business/effect identity. It does not contain attempt counters or clock randomness.

A logical command may have zero or more `CommandAttempt`s over its lifetime. Attempt identity is distinct from effect identity.

Successful stable-ID replay remains an effect-ledger semantic, not generic caching.

### Outcomes

`CommandOutcome<O>` must distinguish at least:

- `Succeeded<O>` with structured confirmation/evidence;
- retryable failure;
- terminal failure;
- ambiguous outcome;
- user action required.

Expected operational states must not be inferred solely from exception classes.

`CommandConfirmation` distinguishes machine evidence such as submitted/provider-acknowledged/read-after-write-verified from user confirmation so attended acknowledgement is not misrepresented as machine verification.

### Retry/idempotency/reconciliation

V1 preserves the current limitation tracked by #545: retained `FAILED_RETRYABLE` effects are not automatically redispatched with the same stable ID.

A future redispatch is conceptually a new `CommandAttempt` under the same `CommandId`, but #545 owns legal transitions/history/redrive authorization.

V1 must not claim automatic redrive where it is not implemented.

### Cache

Generic step-result caching is forbidden for Command execution. It must never suppress, synthesize or replace an external effect or bypass `CommandEffectStore` semantics.

Successful recorded command replay is the only framework-level replay shortcut for command effects in v1.

#575 owns enforcement.

## Query contract

A Query represents a captured external observation, not an effect.

### Outcomes

`QueryOutcome<O>` must distinguish at least:

- found/value;
- not found as a normal semantic outcome;
- temporary unavailability;
- authentication required;
- terminal failure.

`NotFound` is not an accidental provider exception.

### V1 cardinality

Unary Query is an at-most-one outcome. A finite multi-row observation uses
`StreamingQueryOperation` and the existing ONE_TO_MANY runtime; it is not encoded as `List<O>`.

### Capture and cache

Query capture/replay and generic cache reuse are distinct mechanisms.

Semantic query cache identity belongs to Query semantics; `CacheKeyStrategy` may adapt an immutable semantic identity into the cache plugin storage key.

Cache failures are never stored as query facts. Negative caching is explicit opt-in and bounded.

Command IDs/effect state are never query cache identity.

Cross-operation Command→Query invalidation is **not a v1 requirement**. Query cache identities/tags should remain compatible with future explicit invalidation, but M1 does not define an arbitrary invalidation dependency graph.

## Typed configuration

New native providers use immutable typed Java records for provider/operation configuration.

Generated/derived schema metadata must support compiler validation, documentation, MCP/scaffolding and deterministic diagnostics.

Provider/config schema versioning is independent from provider binary versioning. Incompatible config changes require explicit schema-version evolution; silent runtime migration is forbidden.

Schema resources and generated pipeline metadata contain schema identity, sanitized config metadata/digests and references only—not resolved credentials or runtime handles.

## Connections and secrets

`ConnectionRef` is a logical deployment-owned connection name.

`SecretRef` is the only configuration-level secret reference shape. Resolved credentials/tokens/cookies/browser sessions/SDK clients remain runtime-only.

`ConnectionResolver` and `SecretResolver` are lightweight runtime boundaries, **not** OAuth, consent, token-refresh, account-management or vault platforms.

Durable records may contain provider/operation identity, versions, sanitized config digest and only those logical connection identities explicitly required for replay. They must not serialize resolved secrets or runtime handles.

Declaring a correlation or reconciliation reference kind as durable-safe is a provider data-classification decision. Durable reference values are bounded opaque identifiers only; arbitrary evidence, URLs, instructions, credentials, tokens and provider payloads are not reference metadata.

## Compiler/generated-step integration

Preserve distinct `kind: command` and `kind: query` parser/runtime paths.

Native connector selection may add provider/operation/version/typed-config metadata while legacy selectors remain compatible during migration.

Generated Command steps continue through Command-specific runtime support; generated Query steps continue through Query-specific runtime support. Shared provider metadata must not create a generic execution path.

Compiler/build-time validation should use static connector artifact manifests and schemas where possible without constructing providers.

## Compatibility

Existing `CommandConnector<I,O>` remains compatible.

#573 owns the legacy adapter from Mutiny-based CDI `CommandConnector` instances to the new `CompletionStage`-based provider registry. Legacy connectors receive conservative capabilities and are not credited with semantics they cannot prove.

Existing object source/target providers preserve their current streaming/list/read/open-write-session semantics. They may later share provider catalog/lifecycle/config mechanics without being rewritten as unary Query/Command operations.

## Reference mappings

The SPI must remain credible against contrasting providers:

- OpenSearch: automated API-style, concurrent/non-blocking or standard blocking adapter, deterministic external identity where applicable, provider acknowledgement/reconciliation possibilities.
- Stateful browser provider: attended, stateful, serialized/provider-managed execution, weak/no provider idempotency, ambiguous/user-action outcomes and human/browser reconciliation.
- Object source/target: specialised streaming/session ownership preserved.
- Agent: metadata/lifecycle/config seam only until a concrete Agent contract is separately pinned.

## M1 exit condition

M1 is complete when issues #566–#570 agree with this document and are closed as design-resolved.

After M1, implementation issues must treat these decisions as pinned. If repository evidence makes a pinned decision technically impossible, the implementation issue must stop and propose an explicit amendment to this document rather than silently redesigning the SPI.

## Deferred issues

- #545 — controlled redispatch semantics for retained `FAILED_RETRYABLE` command effects.
- #546 — `EventWorkDispatcher.fireAsync` completion/failure semantics.
- #575 — enforce Command exclusion from generic cache.
- #577 — framework-managed execution style/concurrency enforcement.
- Full AgentConnector semantics — deferred until the trigger conditions in #569 are met.
