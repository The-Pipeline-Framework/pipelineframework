# Search Pipeline

This is a generated pipeline application built with the Pipeline Framework.

## Prerequisites

- Java 21
- Maven 3.8+

## Verifying the Generated Application

To verify that the application was generated correctly:

```bash
cd examples/search
./mvnw clean verify
```

This will compile all modules, run tests, and verify that there are no syntax or dependency issues.

## Running the Application

### In Development Mode

Use the Quarkus plugin in IntelliJ IDEA or run with:

```bash
./mvnw compile quarkus:dev
```

### Function Platform Build (Local Mock Runtime)

Build the search pipeline with Function platform mode and RESTful resource naming:

```bash
./build-lambda.sh -DskipTests
```

Run only the Lambda mock event server smoke test:

```bash
./mvnw -pl orchestrator-svc -am \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  compile

./mvnw -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

### Function Platform Build (Azure Functions)

Build the search pipeline for Azure Functions deployment:

```bash
./build-azure.sh -DskipTests
```

Run the Azure Functions bootstrap smoke test:

```bash
./mvnw -pl orchestrator-svc -am \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -DskipTests \
  compile

./mvnw -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -Dtest=AzureFunctionsBootstrapSmokeTest \
  test
```

For local testing with Azure Functions Core Tools:

**Important**: Quarkus dev mode and `quarkus:run` do not work with Azure Functions. The extension requires a staging directory created during deployment. For local runtime testing, use the helper script to prepare the Azure Functions project structure:

```bash
# Build the package
cd examples/search
./mvnw clean package \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -DskipTests

# Prepare Azure Functions project structure (creates host.json, etc.)
./prepare-azure-functions-local.sh

# Run with Azure Functions Core Tools (from examples/search directory where host.json lives)
func host start --java
```

The function will be available at:
- HTTP Trigger URL: `http://localhost:7071/api/{route}`
- Health endpoint: `http://localhost:7071/q/health` (if configured)

**Prerequisites**: Azure Functions Core Tools v4.x must be installed. See [Search Azure Functions Verification Lane](../../docs/guide/build/runtime-layouts/search-azure-functions.md) for installation instructions.

### Function Streaming Lane Status

The search FUNCTION lane now includes explicit fan-out/fan-in path coverage.

- `pipeline.platform=FUNCTION` supports unary and streaming shapes via generated bridge adapters.
- `examples/search/config/pipeline.yaml` now includes:
  - `Tokenize Content`: `ONE_TO_MANY` (runtime/generation shape: `UNARY_STREAMING`)
  - `Index Document`: `MANY_TO_ONE` (runtime/generation shape: `STREAMING_UNARY`)

In this lane:

- `UNARY_UNARY` and `ONE_TO_ONE` are equivalent terms (method shape vs pipeline cardinality term).
- `UNARY_STREAMING` maps to `ONE_TO_MANY`.
- `STREAMING_UNARY` maps to `MANY_TO_ONE`.

Bridge mapping exercised by targeted tests:

- `ONE_TO_MANY` -> `FunctionTransportBridge.invokeOneToMany(...)`
- `MANY_TO_ONE` -> `FunctionTransportBridge.invokeManyToOne(...)`

Runtime parity notes:

- FUNCTION handlers keep the same cardinality contract used by COMPUTE/REST for these shapes.
- `STREAMING_UNARY` handlers reduce non-blockingly (`collect().asList().onItem().transformToUni(...)`).
- `STREAMING_STREAMING` handlers preserve stream-to-stream delegation (`resource::process`) without forced list collection.
- Invalid `tpf.function.invocation.mode` values now fail fast with an explicit error (no silent fallback to LOCAL).

Cardinality guarantees covered by tests:

- `SearchPipelineEndToEndIT#tokenizeAndIndexPersistFanoutBatchesPerDocId` verifies one `docId` flows
  through `RawDocument`/`ParsedDocument`, fans out into multiple persisted `TokenBatch` rows (`tokenBatchCount`),
  then merges into exactly one `IndexAck`.
- `IndexAckResourceTest#testIndexAckRejectsMixedDocIdsInSingleBatch` verifies fan-in rejects mixed `docId` input.

### Branching Reference Lane (Business Semantics)

The search pipeline includes a non-unary business lane:

1. `Tokenize Content` (`ONE_TO_MANY`) expands one `ParsedDocument` into multiple `TokenBatch` units.
2. `Index Document` (`MANY_TO_ONE`) reduces all batches for the same `docId` into one meaningful `IndexAck`.

The reduced `IndexAck` now carries aggregate document signals:

- `tokenBatchCount`: how many batches participated in fan-in.
- `uniqueTokenCount`: unique vocabulary size for the reduced document.
- `topToken`: most frequent token across all batches; when frequencies tie, the lexicographically smallest token is selected.
- fan-in input invariants: each `TokenBatch` must have `batchIndex >= 0` and `tokenCount > 0`; malformed batches fail fast before aggregation.

This keeps the lane business-relevant (document-level indexing summary), not just structural fan-out/fan-in.

### Handler Selection For Modules With Multiple Generated Handlers

Some modules can contain more than one generated function handler (for example, step handlers plus side effect handlers).
In those cases, always select the deployed entrypoint explicitly via:

```properties
%lambda.quarkus.lambda.handler=<fully.qualified.HandlerClassName>
```

Current examples:

- Orchestrator entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.orchestrator.service.PipelineRunFunctionHandler`
- Persistence side effect entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.crawl_source.service.pipeline.PersistenceRawDocumentSideEffectFunctionHandler`
- Cache invalidation entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.cache_invalidation.service.pipeline.CacheInvalidationFunctionHandler`

If handler generation changes, keep this value pinned to the intended runtime entrypoint per module.

## Constructing Crawl Requests

Use the helper to attach fetch options that affect crawl bytes:

```java
import org.pipelineframework.search.common.util.CrawlRequestOptions;

CrawlRequest request = CrawlRequestOptions.builder()
    .fetchMethod("GET")
    .accept("text/html")
    .acceptLanguage("en-US")
    .authScope("tenant-42")
    .header("X-Client-Hint", "mobile")
    .build("https://example.test");
```

## Architecture

This application follows the pipeline pattern with multiple microservices, each responsible for a specific step in the processing workflow. By default, it uses REST transport with resource-oriented endpoints, and the orchestrator coordinates the overall pipeline execution.
