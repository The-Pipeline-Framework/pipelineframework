# Capture, Replay, and Persistence

The JPA query connector is read-side infrastructure. It turns a decision-affecting database read into an explicit captured input for the next business step.

The persistence plugin is write-side infrastructure. It observes pipeline values and stores business outputs for audit, APIs, reports, UIs, and follow-on processing.

Used together, they give a strong state story:

1. Query connector loads and captures the facts a decision used.
2. The decision step stays pure Java over those facts.
3. Persistence stores the resulting business output in the application's durable record.

## Capture and replay

During managed TPF execution, the generated query step computes a capture key from tenant id, execution id, step index, query id, query version, and the selected `capture.keyFields` from the input. The first read result is stored and later retries of the same execution reuse the captured output instead of rereading mutable database state.

If `capture.keyFields` is omitted, the full query input becomes the key input. Prefer explicit key fields when the input contains non-decision metadata.

The in-memory store remains the default for tests and single-process development. Production
deployments can select the DynamoDB store for restart-safe replay and coordination across runtime
replicas. Both stores implement the same Query observation authority.

Provider-backed Query operations use the same capture boundary. `Found` captures the typed output.
`NotFound` captures an explicit absence and replays it as `QueryNotFoundException`; absence is never
represented by `null` or a fabricated output object. Temporary availability, authentication, and
terminal provider failures are not captured as observations.

A finite streaming Query stages its ordered rows during one subscription. Successful terminal
completion atomically commits the observation, including an empty stream. Failure or cancellation
aborts every staged row, so partial capture is never replayable. A retry re-evaluates the source and
may re-emit a prior prefix; stable ONE_TO_MANY lineage identifies that prefix as the same logical
children. Capture does not roll back downstream work already performed by the failed attempt.

Query capture is separate from generic step-result caching:

1. A permitted generic cache hit replays the versioned step output before Query runtime.
2. On a cache miss or bypass, Query runtime checks the execution-scoped capture before resolving a provider.
3. Only when neither replay source supplies an observation does the JPA connector or selected provider execute live.

Consequently, `REQUIRE_CACHE` misses before capture lookup, while `BYPASS_CACHE` bypasses only the
generic cache and can still replay an existing execution capture. The generic cache key, Query
capture key, and live provider identity remain separate contracts.

## Durable DynamoDB capture

Select the durable store at build time and name the pre-provisioned table:

```properties
pipeline.query.capture-store.provider=dynamo
pipeline.query.capture-store.dynamo.table=tpf_query_capture
```

The default provider is `memory`. `custom` disables both built-in beans and requires the
application to supply exactly one `QueryCaptureStore`. Endpoint, region, and credentials use the
standard Quarkus DynamoDB client configuration.

The DynamoDB table is not created by TPF. Provision `capture_key` as the string partition key and
`revision` as the numeric sort key. The runtime identity already hashes tenant, execution, step,
Query descriptor, and selected key input into `capture_key`; the encoded event also retains and
validates the tenant and execution identity. Grant `dynamodb:Query`, `dynamodb:PutItem`,
`dynamodb:Scan`, and `dynamodb:BatchWriteItem`. Reads of the latest authority revision are strongly
consistent and every write is conditional.

Each state change is an immutable revision. Unary `Found` and `NotFound` records survive restart,
including the gap after Query capture and before a generic-cache write. Competing unary calls may
both observe the provider before either stores a result, but they converge on the one conditionally
inserted authority record.

Streaming capture uses one distributed writer lease. Ordered item revisions are invisible to
replay until a terminal commit revision exists; abort and cancellation leave no replayable prefix.
The writer renews its lease while active, and another runtime can reclaim an expired generation
after process loss. Configure the operational timing when needed:

```properties
pipeline.query.capture-store.dynamo.streaming-lease-duration=5m
pipeline.query.capture-store.dynamo.streaming-poll-interval=250ms
```

Durable events retain only a SHA-256 fingerprint of the selected input, never its JSON. This keeps
prompts, credentials, provider configuration, SDK values, runtime handles, and model reasoning out
of the capture table. Canonical outputs retain their declared/runtime types and JSON, sealed-union,
or protobuf encoding. Unknown types, schemas, corrupt rows, and store connectivity failures are
operational failures; they are never converted into Query outcomes or provider misses.

TPF enforces a 300 KiB safe maximum per immutable event, below DynamoDB's item limit. Carry larger
business values through `PayloadReference`. `remove` appends an authoritative tombstone. `clear`
scans and batch-deletes the dedicated table and is intended only for a quiescent maintenance or
test window; normal retention belongs to table lifecycle policy.

## Same database, separate roles

Both the JPA query connector and the persistence plugin can use the same datasource, ORM configuration, and JPA entities in a Quarkus application. They are still separate features:

| Feature | Role | Typical timing |
| --- | --- | --- |
| JPA query connector | Captures read-side facts before a decision | Before a business step |
| Persistence plugin | Stores business outputs for later use | After a step or boundary |

The query connector is not bundled into the persistence plugin and does not require application-supplied connector code.

## Runtime boundaries

The public provider `QueryOperation` and query connector/store contracts use JDK `CompletionStage`
for unary boundaries. The Quarkus JPA connector runs ordinary Jakarta Persistence reads on
framework-owned virtual threads, so applications can share the same JDBC datasource and JPA
entities used by blocking persistence providers without exposing blocking work to pipeline threads.

When the Query output type declares a `mappings.persistence` representation matching the configured
JPA entity, the generated Query client requests that entity and applies the existing
`Mapper.fromExternal(...)` while the JPA persistence context is still open and before Query capture.
This generated mapping path currently applies to `LOCAL` transport; REST and gRPC Query clients
retain their existing representation behavior and do not pass the local mapper callback to
`QueryStepSupport`. Unmapped Query outputs retain record-property projection. In both cases,
captured values use the canonical Query output type.

## Current limits

- `connector: "jpa"` is the only first-party query connector.
- Unary and finite `ONE_TO_MANY` Query runtime capture are supported; first-party database
  `find.many` providers remain separate connector work.
- Java record projection is the supported output shape.
- Predicates are `AND` only; no `OR` groups.
- The database providers currently expose only unary `find.one`; their `find.many` implementations
  have not yet been added to the framework-neutral streaming Query contract.
- JPA declarations still do not support JPQL, named queries, aggregates, optional results, or list results.
- Simple dotted paths are accepted syntactically; invalid JPA paths fail deterministically when the connector executes the query.
