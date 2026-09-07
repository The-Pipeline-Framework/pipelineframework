# Quarkus 3.39 migration

## Release changes

The migration targets Quarkus 3.39.2 on Java 21 across the framework, maintained
examples, and the standalone AI SDK. The starting framework version is 3.33.1;
the AI SDK starts at 3.31.3. Quarkus 3.40 remains a separate upgrade.

Dependency versions come from the Quarkus platform and its aligned Amazon
Services BOM. This removes independent gRPC, protobuf, OpenTelemetry, JUnit, and
Testcontainers pins. LangChain4j stays at 1.19.0 with Quarkus extensions 1.13.1;
its augmentation and connector journeys must pass before release.

Native validation retains Java 21 for Maven compilation and uses Quarkus's
supported container builder. Quarkus 3.36 raised the minimum native builder to
GraalVM/Mandrel 25.0; the former GraalVM for JDK 21 workflow is rejected by 3.39.2.
The workflow now uses property-based native builds without Maven profiles.

Public pipeline YAML, connector operation schemas, cardinality, and durable
record formats are unchanged. PostgreSQL-backed Hibernate applications require
PostgreSQL 14 or newer; examples already use 17. No production schema migration
is prescribed by this change. Applications must validate their own mappings.

See [Dependency Management](/deploy/dependency-management) for application setup.

The generated CLI now uses the Micrometer gRPC interceptor's tag schema and
request-counter unit. See [Metrics](/operate/observability/metrics) for the
direct Micrometer query adjustment. The regression executes generated metric
statements against a real Prometheus registry in both registration orders,
including repeated success and failure recordings.

## Bridge coordination

The separate `tpf-mcp-bridge` checkout still pins Quarkus 3.31.3 in
`mock-scaffold/pom.xml`. Align that scaffold and run its generated-project checks
before claiming generator compatibility with this release. This TPF worktree does
not modify or publish the bridge repository.

## Validation record

Worktree: `pipelineframework-quarkus-3.39.2`, branch `codex/quarkus-3.39.2`.
Baseline commit: `93a53651e`.

Build logs and dependency evidence are collected under
`target/quarkus-migration/` in that worktree. Baseline and upgraded results must
remain distinct. A build that fails before test execution is not a passing
compatibility gate.

The initial framework baseline recorded 3,705 tests in 478 suites, with no failures
or errors and two skips. Final framework verification passed 3,741 tests in 484
suites, with no failures or errors and the same two skips. Both captured connector
semantic contracts remained equal. Final root verification also passed, including
the CSV modular and pipeline-runtime container journeys and the Search pipeline and
replay journeys.

Restaurant Approval passed six maintained tests, Checkout passed 117 tests
including four checkpoint-handoff ITs, and the independent AI SDK passed five
tests. RAG Turnkey augmentation and a Quarkus test calling the injected Ollama
connector against a deterministic streaming HTTP response passed with LangChain4j
1.19.0 / extensions 1.13.1.

Framework verification with the new Failsafe binding includes six LocalStack
classes and 34 tests, including 17 restart journeys. Focused JPA, Hibernate
Reactive, and pgvector checks passed 12, 17, and three tests respectively. The
Prometheus regression passed against a real registry in both registration orders,
with repeated success and failure values and scrape assertions.

Schema export compared all 13 CSV and Search entities with Hibernate ORM 7.2.6
(the Quarkus 3.33.1 baseline) and 7.4.5 (Quarkus 3.39.2). Both exports contained
13 statements and were byte-for-byte identical. No database migration is required
for the repository's maintained mappings.

Search validation passed 12 packaged HTTPS resource ITs across raw document,
parsed document, token batch, and index acknowledgement boundaries. The pipeline
and replay gates passed 13 tests in total, and the Lambda bootstrap smoke passed
one test. The Azure package builds and the current Quarkus Azure bootstrap resource
smoke passed one test without skips. A deployed Azure cloud E2E was not run: the
repository has no `AzureFunctionsEndToEndIT` source, and the workflow requires
external OIDC credentials and provisioned cloud resources.

Validation also exposed pre-existing test gaps. Runtime's six LocalStack IT classes
were not bound to `verify`; they now run through Failsafe. Restart fixtures now
persist and reread await units and decode stored transition payloads before making
semantic assertions. Checkout's request-correlation test now uses the canonical
loader, accepting its existing compact YAML fields. Neither correction changes
production recovery, payload encoding, or pipeline contracts.

Search resource ITs also lacked Failsafe declarations; all four resource service
POMs now bind their existing tests. A successful build with no discovered tests is
excluded from the gate results. Their packaged-app test resource binds explicitly
to loopback HTTPS, which verifies Quarkus 3.38 host validation and avoids selecting
the Azure Functions launcher for ordinary resource tests. The index acknowledgement
fixture now supplies every field required by the generated semantic contract.

The Azure bootstrap smoke previously checked a removed handler class and therefore
skipped silently. It now checks the Quarkus 3.39 Azure middleware and injector
resources. The query-capture reclaim fixture uses its existing mutable clock to
expire the lease deterministically instead of sleeping against a 120 ms wall-clock
lease.

Quarkus 3.39 requires quoted keys for map-valued container-image labels. The CSV
image builders and E2E rebuild path now pass
`quarkus.container-image.labels."label-name"`; the full modular provenance check
passes with the resulting labels.

Container-native validation passed with the Quarkus 3.39 Mandrel 25 builder. The
CSV gate generated the shared module plus native executables for input processing,
payment processing, payment status, and persistence. The Search FUNCTION gate
generated the shared module and the `crawl-source-svc` Lambda executable. Maven
continues to run on Java 21, and both workflow commands use the canonical `clean
package` lifecycle with `quarkus.native.enabled` and container build properties.

## Release gate

The local compatibility gate is complete. Retain the previous application
artifacts for rollback. Run the credentialed Azure deployment workflow before
promoting an Azure-hosted release, and add or restore its missing cloud E2E test.
Any unexpected application-specific database change requires an explicit migration
before release.
