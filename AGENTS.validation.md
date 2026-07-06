# Agent Validation Context

Load this file before choosing validation commands for non-trivial changes.

## Runtime and Build Commands

Framework:

- Build: `./mvnw -f framework/pom.xml clean install`
- Verify: `./mvnw -f framework/pom.xml verify`

Root project:

- Build: `./mvnw clean package`
- Verify: `./mvnw verify`

CSV payments targeted examples:

- Pipeline-runtime orchestrator verification:
  `./examples/csv-payments/build-pipeline-runtime.sh -pl orchestrator-svc -Dcsv.runtime.layout=pipeline-runtime -Dtest=PipelineRuntimeTopologyTest -Dit.test=CsvPaymentsPipelineRuntimeEndToEndIT verify`

- Monolith verification:
  `./examples/csv-payments/build-monolith.sh -DskipTests`

Search targeted example:

- Function platform smoke verification (build-switch based; no Lambda Maven profile):
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -DskipTests compile`
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -Dtest=LambdaMockEventServerSmokeTest test`

Targeted unit-test coverage helper:

- Generate deterministic JaCoCo coverage for a single framework module + test slice:
  `./scripts/coverage-targeted.sh runtime FunctionTransportBridgeTest,UnaryFunctionTransportBridgeTest`
  `./scripts/coverage-targeted.sh deployment RestFunctionHandlerRendererTest`
- Helper output includes report path and LINE/BRANCH percentages from module-local `target/site/jacoco/jacoco.xml`.

Node/docs surfaces:

- AI SDK compile/test surface: `./mvnw -f ai-sdk/pom.xml test`
- Template generator tests live in the separate `The-Pipeline-Framework/tpf-mcp-bridge` repository. The generator-facing schema is exported from `framework/deployment` as `META-INF/pipeline/pipeline-template-schema.json` and consumed by that repo.
- Web UI type/build checks: `npm --prefix web-ui run check`, `npm --prefix web-ui run build`
- Docs build: `npm --prefix docs run build`

## Testing Conventions

- Unit tests: `*Test` (Surefire)
- Integration tests: `*IT` (Failsafe)
- E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.
