package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pipelineframework.orchestrator.ExecutionRedriveIntent;

/**
 * Internal queue-async execution context used by generated await steps.
 */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private final AwaitContinuationMode continuationMode;
    private final TerminalOutputOwnership terminalOutputOwnership;
    private final Map<String, Object> traceMetadata;
    private final ExecutionRedriveIntent redriveIntent;
    private final int redriveStepIndex;
    private final String redriveAdmissionKey;
    private final Set<String> claimedCommandRetries = ConcurrentHashMap.newKeySet();
    private int currentStepIndex;

    public AwaitExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(
            tenantId,
            executionId,
            currentStepIndex,
            AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER,
            Map.of(),
            ExecutionRedriveIntent.REPLAY,
            -1,
            null);
    }

    /**
     * Creates a queue-async context.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param currentStepIndex current pipeline step index
     * @param continuationMode whether an await may use a live completion window
     * @param terminalOutputOwnership side of the transition boundary that publishes terminal output
     */
    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership, Map.of(),
            ExecutionRedriveIntent.REPLAY, -1, null);
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata
    ) {
        this(tenantId, executionId, currentStepIndex, continuationMode, terminalOutputOwnership, traceMetadata,
            ExecutionRedriveIntent.REPLAY, -1, null);
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata,
        ExecutionRedriveIntent redriveIntent,
        int redriveStepIndex
    ) {
        this(
            tenantId,
            executionId,
            currentStepIndex,
            continuationMode,
            terminalOutputOwnership,
            traceMetadata,
            redriveIntent,
            redriveStepIndex,
            redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
                ? "legacy-command-retry:" + executionId + ":" + redriveStepIndex
                : null);
    }

    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership,
        Map<String, Object> traceMetadata,
        ExecutionRedriveIntent redriveIntent,
        int redriveStepIndex,
        String redriveAdmissionKey
    ) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentStepIndex = currentStepIndex;
        this.continuationMode = java.util.Objects.requireNonNull(continuationMode, "continuationMode must not be null");
        this.terminalOutputOwnership = java.util.Objects.requireNonNull(
            terminalOutputOwnership,
            "terminalOutputOwnership must not be null");
        this.traceMetadata = Map.copyOf(java.util.Objects.requireNonNull(traceMetadata, "traceMetadata must not be null"));
        this.redriveIntent = java.util.Objects.requireNonNull(redriveIntent, "redriveIntent must not be null");
        if (redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND && redriveStepIndex < 0) {
            throw new IllegalArgumentException("redriveStepIndex must be non-negative for deliberate Command retry");
        }
        if (redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
            && (redriveAdmissionKey == null || redriveAdmissionKey.isBlank())) {
            throw new IllegalArgumentException("redriveAdmissionKey must not be blank for deliberate Command retry");
        }
        this.redriveStepIndex = redriveIntent == ExecutionRedriveIntent.REPLAY ? -1 : redriveStepIndex;
        this.redriveAdmissionKey = redriveIntent == ExecutionRedriveIntent.REPLAY ? null : redriveAdmissionKey;
    }

    public String tenantId() {
        return tenantId;
    }

    public String executionId() {
        return executionId;
    }

    public int currentStepIndex() {
        return currentStepIndex;
    }

    /**
     * Returns the await continuation mode for this transition.
     *
     * @return continuation mode
     */
    public AwaitContinuationMode continuationMode() {
        return continuationMode;
    }

    /**
     * Returns which side of the boundary owns terminal object publication.
     *
     * @return terminal output owner
     */
    public TerminalOutputOwnership terminalOutputOwnership() {
        return terminalOutputOwnership;
    }

    public Map<String, Object> traceMetadata() {
        return traceMetadata;
    }

    public ExecutionRedriveIntent redriveIntent() {
        return redriveIntent;
    }

    public int redriveStepIndex() {
        return redriveStepIndex;
    }

    public String redriveAdmissionKey() {
        return redriveAdmissionKey;
    }

    public boolean targetsCurrentStepForCommandRetry() {
        return redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
            && currentStepIndex == redriveStepIndex;
    }

    /**
     * Claims the deliberate retry admission once for each logical Command effect at the target step.
     */
    public boolean claimCommandRetry(String commandId) {
        if (commandId == null || commandId.isBlank() || !targetsCurrentStepForCommandRetry()) {
            return false;
        }
        return claimedCommandRetries.add(commandId);
    }

    /** Returns the stable effect-attempt identity for one admitted execution retry. */
    public String commandRetryAttemptId(String commandId) {
        if (commandId == null || commandId.isBlank() || !targetsCurrentStepForCommandRetry()) {
            throw new IllegalStateException("No deliberate Command retry targets the current step");
        }
        UUID identity = UUID.nameUUIDFromBytes(
            (redriveAdmissionKey + "\u0000" + commandId).getBytes(StandardCharsets.UTF_8));
        return "attempt-" + identity;
    }

    public void currentStepIndex(int currentStepIndex) {
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.currentStepIndex = currentStepIndex;
    }
}
