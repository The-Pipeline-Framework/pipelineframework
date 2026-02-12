# Customization points

TPF generates the baseline runtime and adapters, but you still have clear places to inject project-specific behavior when needed.

## Where customization usually happens

### Orchestrator runtime

The orchestrator is generated, but you can extend or replace parts of it when you need stricter control over execution flow.

Common cases:

1. Custom input provisioning (CLI, files, queues, APIs).
2. Extra logging/metrics around pipeline execution.
3. Retry and backoff policies tailored to your domain.

You can also bypass `OrchestratorApplication` and drive the pipeline through custom REST orchestration logic.

### Reactive services

Reactive step services expose a `process()` contract.
You can wrap that behavior without changing pipeline contracts.

Common cases:

1. Add Micrometer timers/counters.
2. Validate inputs before processing.
3. Map infrastructure errors to domain-friendly failures.
4. Add rate limiting or circuit breaking.

### Client steps

Generated client steps are used by the orchestrator to invoke backend services.
They are a good place for call-specific concerns.

Common cases:

1. Add metrics around remote calls.
2. Apply retries/fallbacks.
3. Enrich headers or request metadata before invoking gRPC/REST.

### REST resources

Generated REST resources are regular classes and can be extended safely.

Common cases:

1. Add custom endpoints next to generated `/process`.
2. Customize exception mapping.
3. Inject additional cross-cutting collaborators.

## Practical guidance

- Start from generated behavior first; customize only where you have a clear operational or domain need.
- Keep custom logic close to the relevant boundary (orchestrator, service, client step, resource).
- Prefer wrapper/decoration patterns over invasive edits to generated code.

