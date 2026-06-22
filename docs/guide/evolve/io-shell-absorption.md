# I/O Shell Absorption

TPF should not compete with Spring, Quarkus, or Java APIs as a replacement wrapper. Its stronger adoption path is absorbing imperative and distributed-systems shell concerns when the framework can remove real plumbing: durable state, correlation, replay behavior, duplicate handling, retry/DLQ, and observability.

Await steps are the benchmark. Without TPF, an application owns pending interaction records, callback APIs, correlation, continuation, timeout, duplicate completion handling, and transport-specific async completion. With TPF, those concerns collapse into a framework-owned await boundary plus interaction APIs.

## Priority Table

| I/O shell concern | TPF facility | TPF role | Adoption ROI | Rationale |
| --- | --- | --- | --- | --- |
| Await external/human completion | Await step, interaction API, durable resume | DSL step and runtime primitive | Very high | Absorbs correlation, continuation, pending state, duplicate completion, timeout, and async transports. |
| Decision reads from mutable stores | Query/snapshot step | DSL step and runtime connector | Very high | Turns hidden database reads into explicit replayable pipeline inputs. |
| Idempotent external commands | Command/outbox step | DSL step and runtime connector | Very high | Absorbs command id, effect log, duplicate policy, retry/DLQ, and recorded-result replay. |
| Object/file ingest | Object/file connector steps | DSL step and runtime connector | High | Removes listing, checkpointing, ETag/version capture, dedupe, and quarantine glue. |
| Ordinary API/client calls | Existing Quarkus/Spring/Java clients | Pass-through plus guardrails | Low to medium | TPF should intervene only when replay, durability, or distributed reliability semantics matter. |

## Command/Outbox Step

Command steps are for externally visible effects that must be replay-safe. They are not a new business-service interface and should not add a TPF context parameter to `ReactiveService`. Business transitions stay pure; generated command steps call framework command support, and command support calls a connector.

V1 semantics:

- `kind: command` supports `ONE_TO_ONE` only.
- Authored YAML identifies the semantic command, command id generator, and duplicate policy.
- Endpoint, credentials, index names, provider timeouts, and provider retry tuning live in connector/runtime config.
- Command support requires queue-async execution context.
- `RETURN_RECORDED` returns the recorded output without reissuing the external effect.
- `FAIL` treats a completed duplicate as a terminal duplicate error.
- Connector failures are recorded and rethrown so existing queue-async retry/DLQ handling remains the failure policy owner.

```yaml
- name: Write Search Index Document
  kind: command
  command: opensearch-index-document
  cardinality: ONE_TO_ONE
  input: org.pipelineframework.search.common.domain.SearchIndexDocument
  output: org.pipelineframework.search.common.domain.SearchIndexWriteResult
  commandIdGenerator: org.pipelineframework.search.common.command.SearchIndexDocumentCommandIdGenerator
  duplicatePolicy: RETURN_RECORDED
```

## OpenSearch Proof

Search indexing is the proof because it is a real external mutation where replay and duplicate writes are easy to fake accidentally. The lane separates projection, effect, and aggregation:

```text
Embed Content
  -> Build Search Index Document        ONE_TO_ONE, pure
  -> Write Search Index Document        ONE_TO_ONE, command
  -> Summarize Index Writes             MANY_TO_ONE, pure
```

The separation matters:

- `Build` deterministically projects `EmbeddedChunk` into `SearchIndexDocument`.
- `Write` executes the OpenSearch command with deterministic command id, effect log, duplicate policy, and connector-owned provider details.
- `Summarize` reduces recorded `SearchIndexWriteResult` values back to the existing `IndexAck`.

The deterministic external id and command id are derived from `docId + batchIndex + vectorVersion + vectorHash`, with the command name included in the command id to avoid cross-command collisions. `SearchIndexWriteResult` records `commandId`, `externalId`, `indexName`, `resultStatus`, `createdOrUpdated`, and `recordedDuplicate`.

This proof indexes text/hash metadata only. It should not be described as vector-search support until Search carries actual vector arrays.

## Guardrails

- Use command steps when duplicate suppression and recorded external acknowledgement are framework concerns.
- Keep pure fan-in/fan-out services as ordinary `ReactiveService` or streaming services.
- Do not hide database reads or external writes inside business decision steps when those reads/writes affect replay semantics.
- Do not make command YAML a vendor config blob; put provider configuration in connector/runtime config.
- Add bulk command support only after one-command-per-effect semantics are proven.
