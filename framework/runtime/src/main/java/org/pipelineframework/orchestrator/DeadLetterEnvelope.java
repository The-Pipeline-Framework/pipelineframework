package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Terminal failure payload for dead-letter publishing.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param executionKey deterministic execution key (idempotency/correlation)
 * @param correlationId correlation identifier for cross-system traceability
 * @param transitionKey transition idempotency key
 * @param resourceType failure resource type (for example orchestrator execution)
 * @param resourceName failure resource name (for example service/method)
 * @param transport transport path identifier (REST/GRPC/FUNCTION/LOCAL)
 * @param platform platform identifier (COMPUTE/FUNCTION)
 * @param terminalStatus terminal execution status when moved to DLQ
 * @param terminalReason terminal classification (retry_exhausted/non_retryable)
 * @param errorCode error code
 * @param errorMessage error message
 * @param retryable whether the originating failure class is retryable
 * @param retriesObserved retries observed before terminal transition
 * @param createdAtEpochMs event creation timestamp
 */
public record DeadLetterEnvelope(
    String tenantId,
    String executionId,
    String executionKey,
    String correlationId,
    String transitionKey,
    String resourceType,
    String resourceName,
    String transport,
    String platform,
    String terminalStatus,
    String terminalReason,
    String errorCode,
    String errorMessage,
    boolean retryable,
    int retriesObserved,
    long createdAtEpochMs
) {

    /**
     * Deprecated positional constructor kept for binary/source compatibility while callers migrate to {@link Builder}.
     */
    @Deprecated(forRemoval = false, since = "26.2.5")
    public DeadLetterEnvelope {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(transitionKey, "transitionKey must not be null");
        Objects.requireNonNull(errorCode, "errorCode must not be null");
        if (retriesObserved < 0) {
            throw new IllegalArgumentException("retriesObserved must be >= 0");
        }
    }

    /**
     * Creates a fluent builder for safer envelope construction.
     *
     * @return dead-letter envelope builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link DeadLetterEnvelope}.
     */
    public static final class Builder {
        private String tenantId;
        private String executionId;
        private String executionKey;
        private String correlationId;
        private String transitionKey;
        private String resourceType;
        private String resourceName;
        private String transport;
        private String platform;
        private String terminalStatus;
        private String terminalReason;
        private String errorCode;
        private String errorMessage;
        private boolean retryable;
        private int retriesObserved;
        private long createdAtEpochMs;

        private Builder() {
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder executionId(String executionId) {
            this.executionId = executionId;
            return this;
        }

        public Builder executionKey(String executionKey) {
            this.executionKey = executionKey;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder transitionKey(String transitionKey) {
            this.transitionKey = transitionKey;
            return this;
        }

        public Builder resourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public Builder resourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder transport(String transport) {
            this.transport = transport;
            return this;
        }

        public Builder platform(String platform) {
            this.platform = platform;
            return this;
        }

        public Builder terminalStatus(String terminalStatus) {
            this.terminalStatus = terminalStatus;
            return this;
        }

        public Builder terminalReason(String terminalReason) {
            this.terminalReason = terminalReason;
            return this;
        }

        public Builder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder retryable(boolean retryable) {
            this.retryable = retryable;
            return this;
        }

        public Builder retriesObserved(int retriesObserved) {
            this.retriesObserved = retriesObserved;
            return this;
        }

        public Builder createdAtEpochMs(long createdAtEpochMs) {
            this.createdAtEpochMs = createdAtEpochMs;
            return this;
        }

        public DeadLetterEnvelope build() {
            Objects.requireNonNull(tenantId, "tenantId must not be null");
            Objects.requireNonNull(executionId, "executionId must not be null");
            Objects.requireNonNull(transitionKey, "transitionKey must not be null");
            Objects.requireNonNull(errorCode, "errorCode must not be null");
            if (retriesObserved < 0) {
                throw new IllegalArgumentException("retriesObserved must be >= 0");
            }
            return new DeadLetterEnvelope(
                tenantId,
                executionId,
                executionKey,
                correlationId,
                transitionKey,
                resourceType,
                resourceName,
                transport,
                platform,
                terminalStatus,
                terminalReason,
                errorCode,
                errorMessage,
                retryable,
                retriesObserved,
                createdAtEpochMs);
        }
    }
}
