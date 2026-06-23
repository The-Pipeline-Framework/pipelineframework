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

## Required Runtime Pieces

| Piece | Purpose |
| --- | --- |
| `CommandIdGenerator<I>` | Builds the deterministic command id from the input. |
| `CommandConnector<I, O>` | Calls the external system and returns the command output. |
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
| Connector throws a retryable failure | Effect is marked retryable and queue-async retry policy applies. |
| Connector throws a non-retryable failure | Effect is marked terminal/DLQ. |

The external system still needs an idempotency key or deterministic external id. TPF can avoid repeat dispatch after success is recorded, but it cannot make a third-party system exactly-once.

## Related Docs

- [Writing Command Connectors](/develop/extension/command-connectors)
- [Queue-Async Runtime](/deploy/orchestrator-runtime/queue-async)
- [Replay & Live Topology](/operate/observability/replay)
- [Functional Core, Imperative Shell](/design/fcis)
