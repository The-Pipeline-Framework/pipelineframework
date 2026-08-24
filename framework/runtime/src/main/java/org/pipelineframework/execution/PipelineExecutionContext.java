package org.pipelineframework.execution;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/** Framework-managed execution scope for steps that need durable pipeline identity. */
public record PipelineExecutionContext(
    String tenantId,
    String executionId,
    int currentStepIndex,
    Optional<CommandRetryAdmission> commandRetryAdmission
) {
    public PipelineExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(tenantId, executionId, currentStepIndex, Optional.empty());
    }

    public PipelineExecutionContext {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        commandRetryAdmission = Objects.requireNonNull(
            commandRetryAdmission, "commandRetryAdmission must not be null");
    }

    public static PipelineExecutionContext forCommandRetry(
        String tenantId,
        String executionId,
        int currentStepIndex,
        int commandRetryStepIndex,
        String commandRetryAdmissionKey
    ) {
        return new PipelineExecutionContext(
            tenantId,
            executionId,
            currentStepIndex,
            Optional.of(new CommandRetryAdmission(commandRetryStepIndex, commandRetryAdmissionKey)));
    }

    public PipelineExecutionContext atStep(int stepIndex) {
        return new PipelineExecutionContext(tenantId, executionId, stepIndex, commandRetryAdmission);
    }

    public boolean commandRetryTargetsCurrentStep() {
        return commandRetryAdmission.filter(admission -> admission.targets(currentStepIndex)).isPresent();
    }

    /** Atomically assigns this execution retry to one retryable logical Command effect. */
    public boolean claimCommandRetry(String commandId) {
        return commandRetryAdmission
            .filter(admission -> admission.targets(currentStepIndex))
            .filter(admission -> admission.claim(commandId))
            .isPresent();
    }

    /** Returns the stable effect-attempt identity for the admitted execution retry. */
    public String commandRetryAttemptId(String commandId) {
        return commandRetryAdmission.orElseThrow(() ->
            new IllegalStateException("No deliberate Command retry belongs to this execution"))
            .attemptId(commandId);
    }

    /** Recognizes a recorded result produced by this same execution retry admission. */
    public boolean claimRecordedCommandRetry(String commandId, String attemptId) {
        return commandRetryAdmission
            .filter(admission -> admission.targets(currentStepIndex))
            .filter(admission -> admission.matches(commandId, attemptId))
            .filter(admission -> admission.claim(commandId))
            .isPresent();
    }

    public boolean commandRetryRequested() {
        return commandRetryAdmission.isPresent();
    }

    public boolean commandRetryClaimed() {
        return commandRetryAdmission.filter(CommandRetryAdmission::claimed).isPresent();
    }

    public void requireCommandRetryClaimed() {
        commandRetryAdmission.ifPresent(CommandRetryAdmission::requireClaimed);
    }

    /** One execution-level authorization to retry one persisted logical Command effect. */
    public static final class CommandRetryAdmission {
        private final int targetStepIndex;
        private final String admissionKey;
        private final AtomicReference<String> claimedCommandId = new AtomicReference<>();

        private CommandRetryAdmission(int targetStepIndex, String admissionKey) {
            if (targetStepIndex < 0) {
                throw new IllegalArgumentException("targetStepIndex must be non-negative");
            }
            if (admissionKey == null || admissionKey.isBlank()) {
                throw new IllegalArgumentException("admissionKey must not be blank");
            }
            this.targetStepIndex = targetStepIndex;
            this.admissionKey = admissionKey;
        }

        private boolean targets(int stepIndex) {
            return targetStepIndex == stepIndex;
        }

        private boolean claim(String commandId) {
            return commandId != null && !commandId.isBlank()
                && claimedCommandId.compareAndSet(null, commandId);
        }

        private String attemptId(String commandId) {
            if (!Objects.equals(commandId, claimedCommandId.get())) {
                throw new IllegalStateException("Logical Command effect has not claimed this execution retry");
            }
            return expectedAttemptId(commandId);
        }

        private boolean matches(String commandId, String attemptId) {
            if (commandId == null || commandId.isBlank() || attemptId == null || attemptId.isBlank()) {
                return false;
            }
            return attemptId.equals(expectedAttemptId(commandId));
        }

        private String expectedAttemptId(String commandId) {
            UUID identity = UUID.nameUUIDFromBytes(
                (admissionKey + "\u0000" + commandId).getBytes(StandardCharsets.UTF_8));
            return "attempt-" + identity;
        }

        private boolean claimed() {
            return claimedCommandId.get() != null;
        }

        private void requireClaimed() {
            if (!claimed()) {
                throw new IllegalStateException(
                    "Deliberate Command retry found no FAILED_RETRYABLE logical effect at step " + targetStepIndex);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || other.getClass() != CommandRetryAdmission.class) {
                return false;
            }
            CommandRetryAdmission admission = (CommandRetryAdmission) other;
            return targetStepIndex == admission.targetStepIndex && admissionKey.equals(admission.admissionKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetStepIndex, admissionKey);
        }
    }
}
