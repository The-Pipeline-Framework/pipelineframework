# Orchestrator Queue GA Delivery Map

This page tracks how Queue GA is intentionally sliced into a small number of cohesive PRs.

## Scope of This Delivery Batch

Queue-driven orchestrator GA (without tenant-core SaaS controls) covers:

1. durable queue/state providers,
2. durable dead-letter publishing,
3. crash-recovery semantics validation,
4. transport parity conformance for async contracts.

## Why This Is a 4-PR Stack

The batch is split by production risk boundary, not by file count:

1. `codex/queue-ga-durable-providers-pr4`
2. `codex/queue-ga-final-pr1-durable-dlq`
3. `codex/queue-ga-final-pr2-crash-matrix-tests`
4. `codex/queue-ga-final-pr3-transport-parity-tests`

Each PR is independently reviewable and carries a clear validation gate.

## Workstream to PR Mapping

| Workstream | Primary PR | Why isolated |
|---|---|---|
| Durable state + queue dispatch core | `codex/queue-ga-durable-providers-pr4` | Highest runtime blast radius (OCC, leasing, dispatch, sweeper hooks) |
| Durable DLQ | `codex/queue-ga-final-pr1-durable-dlq` | Keeps dead-letter durability contract independent from core provider wiring |
| Crash semantics and race behavior tests | `codex/queue-ga-final-pr2-crash-matrix-tests` | Locks behavior under crash windows without mixing production logic changes |
| Transport async parity conformance tests | `codex/queue-ga-final-pr3-transport-parity-tests` | Verifies renderer-level async contract parity across REST/gRPC/Function |

## Merge Order

1. Merge durable providers.
2. Merge DLQ durability.
3. Merge crash matrix tests.
4. Merge transport parity conformance tests.

This order keeps lower-level correctness in place before broader conformance coverage.

## Validation Gates

The intended deterministic checks per slice:

1. Runtime provider/DLQ/crash slices:
   `./mvnw -f framework/pom.xml -pl runtime test`
2. Transport parity slice:
   `./mvnw -f framework/pom.xml -pl deployment -Dtest=OrchestratorAsyncTransportParityTest,OrchestratorFunctionHandlerRendererTest,OrchestratorGrpcRendererTest,OrchestratorRestResourceRendererTest test`
3. Search function smoke (for generated function handlers):
   `./mvnw -f examples/search/pom.xml -pl orchestrator-svc -am -Dpipeline.platform=FUNCTION -Dpipeline.transport=REST -Dpipeline.rest.naming.strategy=RESOURCEFUL -DskipTests compile`

## Explicit Non-Goals in This Batch

1. Full event-sourced runtime.
2. Framework-managed compensation engine.
3. Tenant-core SaaS controls (tracked separately as a follow-on feature plan).
