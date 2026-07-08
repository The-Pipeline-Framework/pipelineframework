---
search: false
---

# Coupling Inventory

The scan counted source matches in main framework and plugin source, excluding `target/`, `.git/`, and IDE files.

| Category | Current count | Main hotspots | Migration difficulty |
| --- | ---: | --- | --- |
| Quarkus-specific runtime APIs | 134 matches in 42 runtime files | `PipelineStepResolver`, config classes, gRPC customizers, context filters, `ItemRejectRouter` | Medium |
| Quarkus deployment APIs | 198 matches in 37 deployment files | `OperatorInvokerBuildSteps`, `StepClientRegistrar`, `StepServerRegistrar`, `PipelineFrameworkProcessor` | High but isolated |
| CDI/Jakarta DI and lifecycle | 343 matches in 72 runtime files | `PipelineExecutionService`, `QueueAsyncCoordinator`, `AwaitCoordinator`, `CheckpointPublicationService`, `PipelineRunner` | Medium |
| Mutiny | 1,347 matches in 111 runtime files | `PipelineStepExecutor`, `QueueAsyncCoordinator`, `AwaitStepSupport`, `PipelineExecutionService`, `AwaitCoordinator` | High if removed, medium if adapted |
| Panache/Hibernate Reactive | 0 matches in framework runtime, 8 matches in 2 persistence plugin files | `ReactivePanachePersistenceProvider`, `PersistenceService` | Low to medium |
| Vert.x | 33 matches in 8 runtime files, 27 matches in 3 persistence plugin files, 22 matches in 4 deployment files | context holders, persistence context safety, renderer annotations | Medium |

The runtime module POM is a larger blocker than many individual classes. `framework/runtime/pom.xml` currently pulls Quarkus config YAML, context propagation, Picocli, gRPC, cache, REST, REST client, Micrometer, OpenTelemetry, SmallRye health, DynamoDB, and SQS dependencies. A neutral runtime-core artifact cannot carry this dependency shape.
