# Agent Validation Context

Load this file before choosing validation commands for non-trivial changes.

## Runtime and Build Commands

Framework:

- Build (warm-up worktree): `./mvnw -f framework/pom.xml clean install -DskipTests -Dgpg.skip -Dmaven.repo.local="$PWD/.m2/repository"`
- Verify: `./mvnw -f framework/pom.xml verify -Dmaven.repo.local="$PWD/.m2/repository"`

Root project:

- Build: `./mvnw clean package -Dmaven.repo.local="$PWD/.m2/repository"`
- Verify: `./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"`

CSV payments targeted examples:

- Pipeline-runtime orchestrator verification:
  `./examples/csv-payments/build-pipeline-runtime.sh -pl orchestrator-svc -Dcsv.runtime.layout=pipeline-runtime -Dtest=PipelineRuntimeTopologyTest -Dit.test=CsvPaymentsPipelineRuntimeEndToEndIT verify`

- Monolith verification:
  `./examples/csv-payments/build-monolith.sh -DskipTests`

Search targeted example:

- Function platform smoke verification (build-switch based; no Lambda Maven profile):
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -DskipTests compile -Dmaven.repo.local="$PWD/.m2/repository"`
  `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -Dtest=LambdaMockEventServerSmokeTest test -Dmaven.repo.local="$PWD/.m2/repository"`

Targeted unit-test coverage helper:

- Generate deterministic JaCoCo coverage for a single framework module + test slice:
  `./scripts/coverage-targeted.sh runtime FunctionTransportBridgeTest,UnaryFunctionTransportBridgeTest`
  `./scripts/coverage-targeted.sh deployment RestFunctionHandlerRendererTest`
- Helper output includes report path and LINE/BRANCH percentages from module-local `target/site/jacoco/jacoco.xml`.

Node/docs surfaces:

- AI SDK compile/test surface: `./mvnw -f ai-sdk/pom.xml test -Dmaven.repo.local="$PWD/.m2/repository"`
- Docs tests: `npm --prefix docs test`
- Docs build: `npm --prefix docs run build`

## Testing Conventions

- Unit tests: `*Test` (Surefire)
- Integration tests: `*IT` (Failsafe)
- E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.
- The Unix `mvnw` wrapper exposes the active Docker CLI context as `DOCKER_HOST` when that variable is unset. This lets Testcontainers discover desktop runtimes such as OrbStack without per-shell exports. TLS-enabled TCP contexts also propagate their Docker-managed certificate directory and verification flag; explicitly configured Docker or Testcontainers environment variables always win.
- Use `./tools/full-verify.sh start` for the complete root gate when the caller has a bounded RPC/session window, then poll with `./tools/full-verify.sh status`. The launcher refuses duplicate concurrent runs and a successful log ends with `TPF full verify: all went well`.
