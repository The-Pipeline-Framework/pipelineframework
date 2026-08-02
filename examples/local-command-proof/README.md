# Local Command Proof

This is a deliberately small conformance fixture for generated LOCAL command execution in one Quarkus monolith. It uses queue-async orchestration, in-memory execution and command-effect state, and CDI event dispatch. It does not require an external queue, database, Docker, or connector infrastructure.

## Proven

- A generated `kind: command` step resolves its CDI `CommandConnector` after asynchronous descriptor loading while retaining its queue-async execution context.
- A successful command runs through `LocalPipelineControlPlane`, records a `SUCCEEDED` command effect with typed output, completes the execution, and exposes that typed result through the control plane.
- A successful duplicate with the same stable command ID returns the recorded output without invoking the connector again.
- A non-retryable command failure records a terminal/DLQ command effect and leaves the execution terminal.
- The application-owned fake manager serializes blocking operations independently of command YAML configuration. `maxConcurrency: 1` is connector-visible configuration, not a framework-enforced named-command limit.

## Explicitly Deferred

`FAILED_RETRYABLE` command effects cannot currently be redispatched with the same stable command ID. The focused runtime regression documents that limitation. The disabled monolith retry/redrive journey documents the desired future behavior without claiming it is supported.

`EventWorkDispatcher` acknowledges CDI event dispatch without tracking the `fireAsync` completion stage. Dispatch acknowledgement therefore does not mean the observer completed or that its failure reached the dispatcher caller.

Neither deferred item is solved by this fixture.
