package org.pipelineframework.connector;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable, per-invocation execution metadata. It is intentionally separate from provider-lifetime services.
 */
public record ConnectorExecutionContext(
    Optional<String> tenantId,
    Optional<String> executionId,
    Optional<String> stepId,
    Optional<String> releaseId,
    Optional<String> correlationId,
    Optional<String> traceId,
    Optional<Instant> deadline
) {
    public ConnectorExecutionContext {
        tenantId = nonNull(tenantId, "tenant ID");
        executionId = nonNull(executionId, "execution ID");
        stepId = nonNull(stepId, "step ID");
        releaseId = nonNull(releaseId, "release ID");
        correlationId = nonNull(correlationId, "correlation ID");
        traceId = nonNull(traceId, "trace ID");
        deadline = Objects.requireNonNull(deadline, "deadline must not be null");
    }

    public static ConnectorExecutionContext empty() {
        return new ConnectorExecutionContext(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static Optional<String> nonNull(Optional<String> value, String label) {
        return Objects.requireNonNull(value, label + " must not be null");
    }
}
