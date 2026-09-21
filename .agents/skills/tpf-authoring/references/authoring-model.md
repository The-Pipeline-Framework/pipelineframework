# Authoring model

Read this reference only for canonical types, service/operator shape, cardinality,
branching, composition, recursion, failure handling, or tests. Verify exact syntax
and current support in the template DSL, authoring docs, compiler, and focused tests.

## Model the value first

Choose the smallest canonical type that preserves business meaning:

- record/product: several named facts travel together;
- wrapper: a scalar has a distinct nominal identity (`OrderId` is not `CustomerId`);
- alias: a second name intentionally remains assignable as the target;
- union: a closed set of business outcomes;
- repeated field: an ordered finite collection inside one value.

Do not change a canonical type merely to satisfy a local SDK, database, or transport.
That is a representation problem. Do not use a union as a predicate graph: variants
are named outcomes, and applicability is type-based.

If a step consumes the entire union, omit `accepts`. If it handles selected variants,
narrow with `accepts` or a concrete variant input. The compiler must be able to prove
that reachable terminal alternatives are covered. Let non-applicable values pass
through framework routing; do not rebuild that routing inside the service/operator.

## Choose transformation and cardinality

Decide from the logical flow, before choosing a Java interface:

| Logical shape | Cardinality | Reactive family | Blocking family |
| --- | --- | --- | --- |
| one input -> one result | `ONE_TO_ONE` | unary / `Uni` | synchronous unary |
| one input -> output stream | `ONE_TO_MANY` | server stream / `Multi` | list or incremental iterator |
| input stream -> one result | `MANY_TO_ONE` | client stream / `Uni` | materialized input list |
| input stream -> output stream | `MANY_TO_MANY` | bidirectional / `Multi` | materialized lists |

Use the current supported interface names from `docs/develop/code-a-step.md`; do not
freeze their inventory here. Keep YAML cardinality aligned with the implementation.
TPF does not automatically expand a repeated `List<T>` into a stream or collect a
stream into a repeated field. When that conversion is intended, write it explicitly.

Today the reactive family is `ReactiveService`, `ReactiveStreamingService`,
`ReactiveStreamingClientService`, and `ReactiveBidirectionalStreamingService`. The
blocking family mirrors those shapes and adds `BlockingIteratorService` for incremental
one-to-many work. Re-check the current authoring page before choosing an exact contract.

Prefer reactive code for async APIs, live streams, and end-to-end backpressure. Choose
blocking code when the real library is synchronous. A list-returning blocking shape
loads a batch and trades away live backpressure. Prefer the supported iterator/cursor
shape for incremental `ONE_TO_MANY` synchronous sources, while remembering that a
blocking library may still buffer or perform eager I/O. TPF generates the reactive
bridge/offload; authored code should not create its own executor.

## Ordinary service or operator

Use an ordinary `service:` step when the code is an application-owned transformation
over application/canonical values. YAML owns its contract; the annotation is discovery
plus Java-local execution hints.

Use an `operator:` when the unit is deliberately reusable/delegated, has an unambiguous
public method contract, and one or more of these are true:

- it is packaged for reuse across applications;
- it owns an external DTO/entity model distinct from the application domain;
- it is independently selectable and may move behind a remote boundary;
- pair-accurate mapping at its boundary is part of the design.

Do not introduce an operator for a helper method, one application's policy, DI
convenience, or a generic escape hatch. A Query observes; a Command causes an effect;
an Await suspends. Naming those an operator would erase their replay/durability rules.

Operator selection and signature resolution are build-time concerns. Prefer the
generated adapter and explicit mapper boundary. Keep operators self-contained from
application types when they own a separate reusable model. Do not implement union
dispatch, retries, transport, or pass-through routing inside them when TPF owns it.

## Compose typed flows

A named nested pipeline is an ordinary typed step with its declared input/output. Use
it for a meaningful subflow, not for every helper call. A local nested call continues
inside the same execution; it is not a new workflow row or publication boundary.

Direct self-recursion is structured nested invocation with a typed base case, not a
jump to an earlier step. This makes looping possible in TPF: a union decision routes
either to the recursive call or to the base case, and every invocation later unwinds
through its caller's remaining steps. An agentic loop should therefore carry trusted
typed state, use Query for one new model or read-only tool observation, use Command for
a tool effect, return a continue/complete union, and recurse explicitly for the next
turn. Do not hide the loop or repair attempts inside one Query.
Current cardinality, depth, mutual-recursion, Await, and remote-composition limits are
release-specific. Search the template DSL plus `PipelineRecursion*` and local
invocation/linker tests before depending on them.

## Choose the failure channel

- Expected business outcome: return a typed value/union.
- One bad item that may be tracked while the stream continues: use the supported item
  rejection path.
- Systemic invocation/execution failure: let the runtime classify, retry, and DLQ it.
- Operational re-drive: use a supported typed/admin boundary; do not thread retry
  variants through every happy-path type.

Do not catch an infrastructure failure and convert it into a false business success.
Do not build an application retry ledger beside runtime execution state.

## Compile and test the contract

Unit-test pure service/operator logic and authored mappers without starting the world.
Then compile the real `pipeline.yaml` so service resolution, type applicability,
cardinality, mapper pairs, provider capabilities, and generated artifacts are checked.
Add an integration/topology test only for the boundary/layout the application uses.
Treat compiler diagnostics as design feedback, not a reason to add reflection or glue.

### Model distributed consistency as business flow, not a local transaction

Do not project local database ACID semantics across a distributed TPF business process.

A local transaction is appropriate inside one genuine local persistence/provider authority. It is not the consistency model for a business operation that crosses Pipelines, Commands, providers, runtimes, or independently durable boundaries.

For distributed business progress, prefer TPF's native model:

```text
stable typed result
    -> durable checkpoint/publication
    -> framework-owned downstream admission
    -> stable logical identity / idempotent boundary
    -> next Pipeline
```

Each successfully admitted checkpoint establishes durable business progress. Downstream retry and failure ownership begin at the appropriate handoff boundary; do not hold database locks, invent an application transaction coordinator, or require unrelated TPF authorities to participate in one ACID commit.

When a later business action can fail after earlier durable effects have become authoritative, model the required reversal or resolution explicitly as business semantics. Use a compensation Pipeline when the domain requires compensating previously committed business progress.

```text
Pipeline A succeeds
    -> checkpoint A

Pipeline B succeeds
    -> checkpoint B

Pipeline C fails
    -> compensation Pipeline
    -> explicit compensating business outcome
```

Compensation is not technical rollback and does not pretend that earlier distributed effects never happened. It is a typed, observable business process that restores or establishes an acceptable business state.

Before inventing a transactional application store, distributed lock, rollback coordinator, or provider that atomically spans multiple business authorities, ask:

1. Is this genuinely one local persistence aggregate under one provider authority? If so, local transactional persistence may be appropriate.
2. Has business progress already crossed an independently durable TPF or external-effect boundary? If so, use stable identity, idempotent handoff, retry/replay and explicit compensation where the domain requires reversal.
3. Is the requested "atomicity" actually a terminal business invariant that can be represented by typed intermediate/terminal states rather than one technical transaction?
4. Does an existing TPF checkpoint/publication, persistence, Command effect, Await, retry/DLQ, or compensation pattern already own the required guarantee?

Do not call the absence of one global ACID authority a framework gap merely because several durable business facts participate in one user journey. Prove that the required business invariant cannot be expressed through existing TPF authorities and compensation semantics before proposing new framework or application transaction infrastructure.

For the canonical cross-Pipeline precedent, inspect the TPFGo checkout flow: typed checkpoint publications/subscriptions provide reliable cross-Pipeline progress with explicit idempotency at handoff boundaries, while the compensation Pipeline models business recovery after previously committed progress.

