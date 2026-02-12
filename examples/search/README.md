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
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  compile

./mvnw -pl orchestrator-svc \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

### Function Streaming Lane Status

The current search Function platform lane is intentionally unary-only.

- `pipeline.platform=FUNCTION` currently supports `UNARY_UNARY` steps only.
- The current `examples/search/config/pipeline.yaml` uses `ONE_TO_ONE` cardinality for all functional steps.
- Non-unary (`ONE_TO_MANY`, `MANY_TO_ONE`, `MANY_TO_MANY`) function bridge E2E is therefore not enabled yet in this reference.

When function streaming/cardinality support is expanded, add one explicit non-unary path and a dedicated end-to-end test lane.

### Handler Selection For Modules With Multiple Generated Handlers

Some modules can contain more than one generated function handler (for example, step handlers plus side-effect handlers).
In those cases, always select the deployed entrypoint explicitly via:

```properties
%lambda.quarkus.lambda.handler=<fully.qualified.HandlerClassName>
```

Current examples:

- Orchestrator entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.orchestrator.service.PipelineRunFunctionHandler`
- Persistence side-effect entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.crawl_source.service.pipeline.PersistenceRawDocumentSideEffectFunctionHandler`
- Cache invalidation entrypoint:
  - `%lambda.quarkus.lambda.handler=org.pipelineframework.search.cache_invalidation.service.pipeline.CacheInvalidationFunctionHandler`

If handler generation changes, keep this value pinned to the intended runtime entrypoint per module.

## Constructing crawl requests

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
