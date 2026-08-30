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
    Optional<String> pipelineId,
    Optional<String> contractVersion,
    Optional<String> releaseVersion,
    Optional<String> stepId,
    Optional<ConnectorInvocationTarget> invocationTarget,
    Optional<String> correlationId,
    Optional<String> traceId,
    Optional<Instant> deadline
) {
    public ConnectorExecutionContext {
        tenantId = text(tenantId, "tenant ID");
        executionId = text(executionId, "execution ID");
        pipelineId = text(pipelineId, "pipeline ID");
        contractVersion = text(contractVersion, "contract version");
        releaseVersion = text(releaseVersion, "release version");
        stepId = text(stepId, "step ID");
        invocationTarget = Objects.requireNonNull(invocationTarget, "connector invocation target must not be null");
        correlationId = text(correlationId, "correlation ID");
        traceId = text(traceId, "trace ID");
        deadline = Objects.requireNonNull(deadline, "deadline must not be null");
    }

    public static ConnectorExecutionContext empty() {
        return new ConnectorExecutionContext(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ConnectorExecutionContext managed(
        String tenantId,
        String executionId,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        String stepId,
        ConnectorInvocationTarget invocationTarget,
        Optional<String> correlationId,
        Optional<String> traceId,
        Optional<Instant> deadline
    ) {
        return new ConnectorExecutionContext(
            Optional.of(tenantId),
            Optional.of(executionId),
            Optional.of(pipelineId),
            Optional.of(contractVersion),
            Optional.of(releaseVersion),
            Optional.of(stepId),
            Optional.of(invocationTarget),
            correlationId,
            traceId,
            deadline);
    }

    public static ConnectorExecutionContext forTarget(
        String stepId,
        ConnectorInvocationTarget invocationTarget
    ) {
        return new ConnectorExecutionContext(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(stepId),
            Optional.of(invocationTarget),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    }

    private static Optional<String> text(Optional<String> value, String label) {
        Optional<String> actual = Objects.requireNonNull(value, label + " must not be null");
        actual.ifPresent(text -> {
            if (text.isBlank() || !text.equals(text.trim())) {
                throw new IllegalArgumentException(label + " must be non-blank without surrounding whitespace");
            }
        });
        return actual;
    }
}
