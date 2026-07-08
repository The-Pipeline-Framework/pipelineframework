---
search: false
---

# Writing Command Connectors

Command connectors adapt a typed pipeline command to an external system. Use one when the effect benefits from command semantics: command id, effect logging, duplicate policy, recorded-output replay, retry/DLQ handling, and telemetry.

For YAML setup, see [Command Steps](/versions/v26.6.2/deploy/orchestrator-runtime/command). This page covers the Java code.

## What You Implement

A command step needs two application classes:

1. `CommandIdGenerator<I>` for the command input type
2. `CommandConnector<I, O>` for the command input and output types

Keep both classes typed. Do not implement command connectors as `CommandConnector<Object, Object>`.

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
