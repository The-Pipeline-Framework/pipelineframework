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
public provider contract. Authors implement provider identity/version, an operation catalog, typed
configuration records when needed, and the family-specific operation method. Lifecycle methods are
optional no-ops. Do not construct provider/operation descriptors or write provider factories.
Configure a provider instance once under top-level `connectors`, then select an operation from a
Command step with `operation` and `using`:

```yaml
connectors:
  search:
    provider: acme.search
    version: 1
    config:
      connection: search-primary

steps:
  - name: Write document
    kind: command
    operation: write.document
    operationVersion: 1
    using: search
    commandIdGenerator: com.example.DocumentCommandIdGenerator
    config:
      index: orders
    policy:
      requireIdempotency: true
      requireReconciliation: true
      requiredExecutionPosture: AUTOMATED
      requiredExecutionStyle: PROVIDER_MANAGED
      requiredConcurrencyScope: PROVIDER_MANAGED
      minimumMachineConfirmation: PROVIDER_ACKNOWLEDGED
```

`connectors.search.config` is provider-lifetime configuration. The step's `config` is operation
configuration; the pipeline item remains the dynamic invocation input. Every step using `search`
shares that named binding, while different bindings receive distinct provider instances. Bindings
activate on first live use, so replay of an already recorded outcome does not start the provider.

`using` is a pipeline-local binding name, not a provider ID. `ConnectionRef` and `SecretRef` values
remain logical deployment-owned references and are resolved only at provider start or operation
invocation, never while parsing or compiling. `operationVersion` defaults to `1` when omitted.

The deprecated provider-first form remains readable during migration and is not silently
reinterpreted. Select it instead of `command`, never alongside it:

```yaml
connector:
  provider: acme.search
  providerVersion: 1
  operation: write.document
  operationVersion: 1
  policy:
    requireIdempotency: true
    requireReconciliation: true
    requiredExecutionPosture: AUTOMATED
    requiredExecutionStyle: PROVIDER_MANAGED
    requiredConcurrencyScope: PROVIDER_MANAGED
    minimumMachineConfirmation: PROVIDER_ACKNOWLEDGED
```

Prefer named bindings for new pipelines. Operation IDs remain provider-scoped; selecting
`write.document` through `search` does not make it a provider-independent operation contract.

TPF packaging derives provider metadata and direct service registration from the executable provider.
The resulting `META-INF/pipeline/connector-providers.json` is validated during consumer compilation;
the provider is not constructed by that consumer-side check. TPF binds provider and operation
configuration independently to their declared immutable configuration records before
an effect is created or the operation is invoked.

Bind the packaging goal to the provider artifact's canonical lifecycle; it scans the artifact's public,
concrete `ConnectorProvider` implementations after compilation. Provider constructors must therefore be
public and side-effect free; acquire connections and other resources during `start`.

```xml
<plugin>
  <groupId>org.pipelineframework</groupId>
  <artifactId>connector-maven-plugin</artifactId>
  <version>${pipeline-framework.version}</version>
  <executions>
    <execution><goals><goal>generate-provider-artifacts</goal></goals></execution>
  </executions>
</plugin>
```

`CommandOutcome` distinguishes success, retryable failure, terminal failure, ambiguous submission,
and user action required. Only declared safe correlation or reconciliation references, outcome
codes, confirmation strengths, and a redacted configuration digest are retained in the effect
record, together with the selected provider and operation major versions. Evidence, descriptions,
secret references, and resolved handles are not durable metadata.

`AUTOMATED`, `ATTENDED`, and an undeclared conservative posture are operation capabilities; a
pipeline policy may require one explicitly. A successful outcome must also achieve the policy's
minimum machine and user confirmation. Insufficient machine confirmation becomes an `AMBIGUOUS`
barrier, while missing required user confirmation becomes `USER_ACTION_REQUIRED`; neither is
recorded as success or automatically retried.

Declaring a reference kind in `durableReferenceKinds` is a provider data-classification decision.
Values must be bounded opaque identifiers such as `TKT-123`, never credentials, tokens, URLs,
arbitrary evidence, instructions, or provider payloads. TPF filters undeclared kinds and rejects
non-identifier value shapes, but the provider remains responsible for classifying each declared
kind as safe for durable storage.

An existing `SUCCEEDED` record with `RETURN_RECORDED` is replayed before a provider is looked up.
Ordinary admission and ordinary execution re-drive never redispatch a `FAILED_RETRYABLE` record.
The queue-async control plane may deliberately re-drive a failed execution with
`RETRY_FAILED_COMMAND` intent. The execution resumes from its persisted current-step input through
the normal generated Command client, which consumes that intent only at the targeted Command step.
`CommandStepSupport.retry(...)` remains the lower-level runtime primitive: a retry-capable effect
store atomically appends and claims one new attempt under the same logical `CommandId`. `DLQ`,
`AMBIGUOUS`, and `USER_ACTION_REQUIRED` remain barriers. Native commands do not run with
framework-managed blocking execution or bounded framework-managed concurrency.
The transition identity and logical `CommandId` derive a stable attempt identity for that
one admission. If the transition worker is recovered after the attempt has already failed,
the same admission reports the recorded retryable failure instead of appending another attempt.

The stable `CommandId` remains the provider idempotency identity across attempts. Legacy connectors
receive the individual `attemptId` on `CommandRequest`; native operations receive both values through
`CommandInvocation.dispatchIdentity()`. Attempt IDs are diagnostic dispatch identities, not new
logical effects and not provider idempotency keys.

## Native Provider Queries

The same named provider binding can expose a unary `QueryOperation<I, C, O>`. Select it with the
shared operation-first grammar; there is no separate provider-first Query selector:

```yaml
connectors:
  search:
    provider: acme.search
    version: 1

steps:
  - name: Find document
    kind: query
    operation: find.document
    operationVersion: 1
    using: search
    config:
      index: orders
    negativeCacheTtl: PT20S
```

TPF binds `config` to the operation's immutable configuration record before invocation. Provider
authors receive a typed `QueryInvocation` and return a JDK
`CompletionStage<QueryOutcome<O>>`. `Found` supplies the step output. `NotFound` becomes the typed
non-retryable `QueryNotFoundException`; `TemporarilyUnavailable` remains retryable, while
`AuthenticationRequired` and `TerminalFailure` are non-retryable failures. Public provider code
does not depend on Mutiny, CDI, or Quarkus.

Query capabilities are conservative when omitted. `LIVE_ONLY` requires `BYPASS_CACHE`.
`CACHEABLE` permits the ordinary pipeline cache policies; a declared `maximumCacheAge` requires
the configured positive cache TTL to be no greater than that maximum. Without a provider maximum,
a positive TTL is not required.

`negativeCacheTtl` is optional. It is valid only when the operation declares a maximum negative
cache TTL, must not exceed that maximum, and stores only a bounded internal `NotFound` marker.
It does not cache authentication, temporary availability, terminal failures, provider payloads,
or arbitrary metadata.

Pipeline cache replay, execution-scoped Query capture replay, and a live provider observation are
separate paths. A generic cache hit returns before Query runtime. After a cache miss that permits
execution, an existing Query capture is replayed before resolving the provider. Only a miss in
both layers invokes the provider. See [Cache Policies](/design/caching/policies) and
[Capture, Replay, and Persistence](/design/jpa-query-connector/capture-and-persistence).

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

## Pipeline cache replay and Command effect replay

Generated Command steps remain eligible for generic step-result caching. A warm `PREFER_CACHE` or
`REQUIRE_CACHE` hit returns the versioned cached output without entering `CommandStepSupport`; no effect record is
created and no provider is invoked. This is pipeline replay, not evidence of a live external effect.

On a cache miss, `CACHE_ONLY`, or `BYPASS_CACHE`, normal Command execution applies. `CommandId` identifies the
logical external effect and `CommandEffectStore` decides whether to dispatch, return a recorded success, or
preserve a terminal barrier. The generic cache key is a separate replay identity and follows the configured
`CacheKeyStrategy` and version tag.

`SKIP_IF_PRESENT` is not valid for Command steps because it could perform a new live effect while deliberately
leaving an older replay output under the same cache key. Use `PREFER_CACHE`, `REQUIRE_CACHE`, `CACHE_ONLY`, or
`BYPASS_CACHE` instead.

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
