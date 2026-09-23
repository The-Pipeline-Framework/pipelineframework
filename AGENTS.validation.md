# Agent Validation Context

Load this file before choosing validation commands for non-trivial changes.

## Runtime and Build Commands

Framework:

- Build (warm-up worktree): `./mvnw -f framework/pom.xml clean install -DskipTests -Dgpg.skip -Dmaven.repo.local="$PWD/.m2/repository"`
- Verify: `./mvnw -f framework/pom.xml verify -Dmaven.repo.local="$PWD/.m2/repository"`

Root project:

- Build: `./mvnw clean package -Dmaven.repo.local="$PWD/.m2/repository"`
- Verify: `./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"`

CSV Kafka Payments validation is owned by the standalone
[`csv-kafka-payments`](https://github.com/The-Pipeline-Framework/csv-kafka-payments) repository.

Owner-local validation after the split:

- contracts/model/protocol changes: `pipelineframework-contracts`;
- compiler and generated-artifact changes: `pipelineframework-compiler`;
- Quarkus/Spring/runtime/plugin changes: `pipelineframework-runtime`;
- Connector/provider/importer/host changes: `pipelineframework-connectors`;
- Block or Expansion changes: their respective standalone repositories;
- learning proof changes: `pipelineframework-examples`;
- Checkout/TPFGo, Search or QuickBooks changes: `pipelineframework-reference-implementations`;
- real application changes: the application repository.

For a change that crosses a released boundary, owner-local `verify` is only the first gate. Publish/consume the
fresh snapshot and run the smallest affected downstream compatibility or E2E suite. The coordination repository's
transport-completeness tests do not substitute for owner E2E tests.

Node/docs surfaces:

- Docs tests: `npm --prefix docs test`
- Docs build: `npm --prefix docs run build`

## Testing Conventions

- Unit tests: `*Test` (Surefire)
- Integration tests: `*IT` (Failsafe)
- E2E tests using containers should run in `verify` unless there is an explicit reason otherwise.
- A test and the CI invocation that exercises its distinct configuration/topology must move together.
- The Unix `mvnw` wrapper exposes the active Docker CLI context as `DOCKER_HOST` when that variable is unset. This lets Testcontainers discover desktop runtimes such as OrbStack without per-shell exports. TLS-enabled TCP contexts also propagate their Docker-managed certificate directory and verification flag; explicitly configured Docker or Testcontainers environment variables always win.
- Use `./tools/full-verify.sh start` for the complete root gate when the caller has a bounded RPC/session window, then poll with `./tools/full-verify.sh status`. The launcher refuses duplicate concurrent runs and a successful log ends with `TPF full verify: all went well`.
