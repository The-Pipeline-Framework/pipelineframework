package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * Placeholder provider for DynamoDB-backed execution state.
 *
 * <p>The core runtime ships the SPI and an in-memory provider. A production DynamoDB
 * provider is expected to be supplied by deployment-specific modules.</p>
 */
@ApplicationScoped
public class DynamoExecutionStateStore implements ExecutionStateStore {

    private static final String ERROR =
        "DynamoExecutionStateStore is selected but not implemented in core runtime. " +
            "Provide a deployment-specific provider module.";

    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
        String tenantId,
        String executionId,
        long expectedVersion,
        int nextAttempt,
        long nextDueEpochMs,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }

    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().failure(new UnsupportedOperationException(ERROR));
    }
}
