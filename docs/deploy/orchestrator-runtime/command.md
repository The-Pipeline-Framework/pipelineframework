# Command Steps

Command steps record and dispatch external writes from a `QUEUE_ASYNC` pipeline. Use them when a retry or replay must not blindly call the external system again.

Start here when you need to declare a command step in `pipeline.yaml`. For connector code, see [Writing Command Connectors](/develop/extension/command-connectors). For runtime signals, see [Replay & Live Topology](/operate/observability/replay) and [Metrics](/operate/observability/metrics).

## When To Use

Use `kind: command` when the step:

1. writes to an external system,
2. has a deterministic command id,
3. needs duplicate handling on replay,
4. should record the external write result as pipeline output.

Good fits include search indexing, ticket creation, email submission, provider provisioning, or payment-provider commands with idempotency keys.

For application logic that does not need an effect record, keep using a normal `ReactiveService`, operator, or generated adapter. If the external system completes later through a callback, broker response, or human action, configure [Await Runtime Setup](/deploy/orchestrator-runtime/await). When a completed pipeline hands work to another pipeline, model that boundary with [Checkpoint Handoff](/deploy/orchestrator-runtime/checkpoint-handoff).

## YAML

V1 command steps are `ONE_TO_ONE` and require `QUEUE_ASYNC`.

```yaml
steps:
  - name: "Write Search Index Document"
    kind: "command"
    command: "opensearch-index-document"
    cardinality: "ONE_TO_ONE"
    input: "org.pipelineframework.search.common.domain.SearchIndexDocument"
    output: "org.pipelineframework.search.common.domain.SearchIndexWriteResult"
    commandIdGenerator: "org.pipelineframework.search.common.command.SearchIndexDocumentCommandIdGenerator"
    duplicatePolicy: "RETURN_RECORDED"
```

The authored step names the command and the typed input/output contract. Provider details such as endpoint, credentials, index name, timeout, and provider retry tuning belong in runtime configuration.

For a native Connector Provider, declare a named configured instance separately from the operation:

```yaml
connectors:
  search:
    provider: acme.search
    version: 1
    config:
      connection: search-primary

steps:
  - name: "Write Search Index Document"
    kind: command
    operation: write.document
    using: search
    input: SearchIndexDocument
    output: SearchIndexWriteResult
    java:
      input: org.example.SearchIndexDocument
      output: org.example.SearchIndexWriteResult
    commandIdGenerator: org.example.SearchIndexDocumentCommandIdGenerator
    config:
      index: orders
```

The `search` binding starts once and may be reused by several Command operations. Its `config` is
provider-level configuration. The step `config` is operation-level configuration. Static provider
metadata validates both scopes during compilation without constructing the provider or resolving
connection and secret references.

## Required Runtime Pieces

For operation-first native provider commands, implement `CommandOperation<I, C, O>` as described in
[Writing Command Connectors](/develop/extension/command-connectors). The older
`CommandConnector<I, O>` entry below applies only to the retained legacy command path.

| Piece | Purpose |
| --- | --- |
| `CommandIdGenerator<I>` | Builds the deterministic command id from the input. |
| `CommandConnector<I, O>` | Legacy compatibility adapter that calls the external system. |
| `CommandEffectStore` | Records pending, dispatching, success, retryable failure, and DLQ state. |

The generated command step calls these pieces. Application code does not call the effect store directly.

## Duplicate Policy

`RETURN_RECORDED` returns the stored output when the same command id has already succeeded. This is the usual replay-safe setting.

`FAIL` rejects a duplicate successful command. Use it only when a duplicate is a business error and the caller should not receive the earlier result.

## Failure Behavior

| Result | Runtime behavior |
| --- | --- |
| Connector succeeds | Output is recorded and returned. |
| Same command id already succeeded with `RETURN_RECORDED` | Stored output is returned; the connector is not called again. |
| Connector throws a retryable failure | The current attempt is marked `FAILED_RETRYABLE`. Ordinary admission does not redispatch it. |
| Control plane deliberately retries a failed Command execution | The store atomically appends one attempt under the same command id and dispatches through the ordinary provider path. |
| Connector throws a non-retryable failure | Effect is marked terminal/DLQ. |

The external system still needs an idempotency key or deterministic external id. TPF can avoid repeat dispatch after success is recorded, but it cannot make a third-party system exactly-once.

Command-step configuration is immutable application configuration passed to the connector. It does not configure framework-enforced per-command concurrency. Connectors own blocking offload and any provider-specific concurrency limit.

Completed commands with `RETURN_RECORDED` replay their stored output without recalling the connector.
Queue-Async does not automatically retry failed effects. Deliberate retry is admitted only for
`FAILED_RETRYABLE`; terminal, ambiguous, in-flight, and user-action barriers are preserved.
The configured execution store must preserve deliberate retry intent, and the configured
`CommandEffectStore` must advertise retry-attempt history support. Older implementations fail this
operation rather than silently emulating it or discarding attempt identity.

## Related Docs

- [Writing Command Connectors](/develop/extension/command-connectors)
- [Queue-Async Runtime](/deploy/orchestrator-runtime/queue-async)
- [Replay & Live Topology](/operate/observability/replay)
- [Functional Core, Imperative Shell](/design/fcis)
