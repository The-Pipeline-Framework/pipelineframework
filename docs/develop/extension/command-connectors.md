# Writing Command Connectors

Command connectors adapt a typed pipeline command to an external system. Use one when the effect benefits from command semantics: command id, effect logging, duplicate policy, recorded-output replay, retry/DLQ handling, and telemetry.

## Blocking Work And Connector-Owned Limits

`CommandConnector` execution is reactive. A connector that calls a blocking client must explicitly offload that work to an application-owned worker or executor; generated command steps do not offload it automatically. Defer the blocking invocation until subscription, then offload it:

```java
return Uni.createFrom().item(() -> manager.callBlocking(request.input()))
    .runSubscriptionOn(applicationManager.workerExecutor());
```

Do not call the blocking client while constructing the `Uni`; a connector test should assert the executing thread. `CommandStepSupport` captures the execution context before an asynchronously loaded descriptor resolves and supplies it in the `CommandRequest`. Long-lived clients, sessions, and provider-specific concurrency limits belong to the connector or its application-scoped manager.

The command `config` map is immutable connector-visible application configuration. A value such as `maxConcurrency: 1` is not a framework-enforced named-command limit; the connector must implement any such limit itself.

For YAML setup, see [Command Steps](/deploy/orchestrator-runtime/command). This page covers the Java code.

## What You Implement

The existing `CommandConnector<I, O>` path below remains supported without migration. It is the
legacy compatibility path and uses Mutiny inside the Quarkus runtime.

A command step needs two application classes:

1. `CommandIdGenerator<I>` for the command input type
2. `CommandConnector<I, O>` for the command input and output types

Keep both classes typed. Do not implement command connectors as `CommandConnector<Object, Object>`.

## Native Provider Commands

Native connector providers use the host-neutral `CommandOperation<I, C, O>` SPI. They return a
JDK `CompletionStage<CommandOutcome<O>>`; Mutiny, CDI, and Quarkus types do not appear in that
public provider contract. Select one from YAML instead of `command`, never alongside it:

```yaml
connector:
  provider: acme.search
  providerVersion: 1
  operation: write.document
  operationVersion: 1
  policy:
    requireIdempotency: true
    requireReconciliation: true
    requiredExecutionStyle: PROVIDER_MANAGED
    requiredConcurrencyScope: PROVIDER_MANAGED
    minimumMachineConfirmation: PROVIDER_ACKNOWLEDGED
```

Provider metadata in `META-INF/pipeline/connector-providers.json` is validated during compilation;
the provider is not constructed for that check. The operation configuration remains the step's
`config` map, but TPF binds it to the provider's declared immutable configuration record before
an effect is created or the operation is invoked.

`CommandOutcome` distinguishes success, retryable failure, terminal failure, ambiguous submission,
and user action required. Only declared safe correlation or reconciliation references, outcome
codes, confirmation strengths, and a redacted configuration digest are retained in the effect
record. Evidence, descriptions, secret references, and resolved handles are not durable metadata.

An existing `SUCCEEDED` record with `RETURN_RECORDED` is replayed before a provider is looked up.
`FAILED_RETRYABLE` records are deliberately not redispatched in this slice: legal redispatch
transitions are owned by #545. `BLOCKING` execution and bounded framework-managed concurrency are
also deferred to #577.

## Command Id Generator

The command id must be stable for the same business command. Do not include the current time, a random UUID, or a process-local counter.

```java
@ApplicationScoped
public class SearchIndexDocumentCommandIdGenerator
    implements CommandIdGenerator<SearchIndexDocument> {

  @Override
  public String commandId(CommandDescriptor descriptor, SearchIndexDocument input) {
    if (input.docId == null) {
      throw new IllegalArgumentException("docId is required");
    }
    if (input.batchIndex == null || input.batchIndex < 0) {
      throw new IllegalArgumentException("batchIndex must be >= 0");
    }
    if (input.vectorVersion == null || input.vectorVersion.isBlank()) {
      throw new IllegalArgumentException("vectorVersion is required");
    }
    if (input.vectorHash == null || input.vectorHash.isBlank()) {
      throw new IllegalArgumentException("vectorHash is required");
    }

    return descriptor.command() + ":" + sha256Base64Url(String.join("|",
        input.docId.toString(),
        input.batchIndex.toString(),
        input.vectorVersion.trim(),
        input.vectorHash.trim()));
  }
}
```

Include the command name, or another command namespace, so two different commands cannot collide on the same business fields.

## Connector

The connector performs one external write and returns the recorded result.

```java
@ApplicationScoped
public class OpenSearchIndexDocumentCommandConnector
    implements CommandConnector<SearchIndexDocument, SearchIndexWriteResult> {

  @Override
  public String command() {
    return "opensearch-index-document";
  }

  @Override
  public Uni<SearchIndexWriteResult> execute(CommandRequest<SearchIndexDocument> request) {
    SearchIndexDocument input = request.input();

    return upsertIntoOpenSearch(input.externalId, input)
        .map(ignored -> {
          SearchIndexWriteResult result = new SearchIndexWriteResult();
          result.commandId = request.commandId();
          result.externalId = input.externalId;
          result.indexName = input.indexName;
          result.resultStatus = "UPSERTED";
          result.createdOrUpdated = true;
          return result;
        });
  }
}
```

Use `request.commandId()` as the provider idempotency key when the provider supports it. If the provider has its own document id or external id, derive it from the same stable business fields.

## What TPF Handles

The generated command step calls the generator and connector. TPF also handles:

- creating the effect record,
- marking dispatch start,
- recording success output,
- returning stored output for `RETURN_RECORDED`,
- marking retryable failures,
- marking terminal DLQ failures.

The connector should not read or write the `CommandEffectStore` directly.

## Error Classification

Throw a retryable exception for provider failures that may succeed later, such as transient network errors or `5xx` responses.

Throw `NonRetryableException`, or an exception wrapped in `NonRetryableException`, when the same command input cannot succeed without a code, data, or configuration change. Examples include malformed payloads, missing required fields, and provider `4xx` validation errors.

## Configuration

Read provider details from runtime configuration:

```properties
search.index.opensearch.endpoint=http://localhost:9200
search.index.opensearch.index=search-documents
search.index.opensearch.timeout-seconds=5
```

Do not put endpoint URLs, credentials, or provider timeout tuning in the authored step unless the value is part of the pipeline contract.

## Testing

At minimum, test:

1. command id stability for the same input,
2. validation failures before dispatch,
3. provider success mapping to the output type,
4. retryable provider failure classification,
5. non-retryable provider failure classification.

Also test replay behavior through `CommandStepSupport`: with `RETURN_RECORDED`, a second execution for the same command id should return the stored output and should not call the connector again.

## Example

The Search example implements an OpenSearch command connector:

- `examples/search/common/src/main/java/org/pipelineframework/search/common/command/SearchIndexDocumentCommandIdGenerator.java`
- `examples/search/common/src/main/java/org/pipelineframework/search/common/command/OpenSearchIndexDocumentCommandConnector.java`
- `examples/search/common/src/test/java/org/pipelineframework/search/common/command/OpenSearchIndexDocumentCommandConnectorTest.java`
