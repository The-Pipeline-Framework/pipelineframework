package org.pipelineframework.command;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Internal invocation-scoped authority for one control-plane admitted Command retry.
 *
 * <p>This type exists only so framework invocation machinery can capture and restore the
 * authority across reactive boundaries. The application-facing execution context remains
 * immutable and Command-neutral.</p>
 */
public final class CommandRetryExecutionScope {
    private static final String CONTEXT_KEY = CommandRetryExecutionScope.class.getName() + ".admission";

    private CommandRetryExecutionScope() {
    }

    public static Snapshot capture() {
        return new Snapshot(RuntimeAdapters.executionContext(CONTEXT_KEY, Admission.class));
    }

    public static AdmissionHandle installRetry(
        int targetStepIndex,
        String targetCommandId,
        String admissionKey
    ) {
        Admission admission = new Admission(targetStepIndex, targetCommandId, admissionKey);
        RuntimeAdapters.setExecutionContext(
            CONTEXT_KEY,
            admission);
        return new AdmissionHandle(admission);
    }

    public static void clear() {
        RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
    }

    public static void restore(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        if (snapshot.admission == null) {
            clear();
        } else {
            RuntimeAdapters.setExecutionContext(CONTEXT_KEY, snapshot.admission);
        }
    }

    static Optional<String> claimAttempt(int stepIndex, String commandId) {
        return current()
            .filter(admission -> admission.claim(stepIndex, commandId))
            .map(Admission::attemptId);
    }

    static boolean claimRecorded(int stepIndex, String commandId, String attemptId) {
        return current()
            .filter(admission -> admission.claimRecorded(stepIndex, commandId, attemptId))
            .isPresent();
    }

    private static Optional<Admission> current() {
        return Optional.ofNullable(RuntimeAdapters.executionContext(CONTEXT_KEY, Admission.class));
    }

    public static final class Snapshot {
        private final Admission admission;

        private Snapshot(Admission admission) {
            this.admission = admission;
        }
    }

    /** Opaque completion guard retained only by the framework seam that admitted the retry. */
    public static final class AdmissionHandle {
        private final Admission admission;

        private AdmissionHandle(Admission admission) {
            this.admission = admission;
        }

        public void requireConsumed() {
            admission.requireConsumed();
        }
    }

    private static final class Admission {
        private final int targetStepIndex;
        private final String targetCommandId;
        private final String attemptId;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private Admission(int targetStepIndex, String targetCommandId, String admissionKey) {
            if (targetStepIndex < 0) {
                throw new IllegalArgumentException("targetStepIndex must be non-negative");
            }
            if (targetCommandId == null || targetCommandId.isBlank()) {
                throw new IllegalArgumentException("targetCommandId must not be blank");
            }
            if (admissionKey == null || admissionKey.isBlank()) {
                throw new IllegalArgumentException("admissionKey must not be blank");
            }
            this.targetStepIndex = targetStepIndex;
            this.targetCommandId = targetCommandId;
            UUID identity = UUID.nameUUIDFromBytes(
                (admissionKey + "\u0000" + targetCommandId).getBytes(StandardCharsets.UTF_8));
            this.attemptId = "attempt-" + identity;
        }

        private boolean claim(int stepIndex, String commandId) {
            return targetStepIndex == stepIndex
                && targetCommandId.equals(commandId)
                && consumed.compareAndSet(false, true);
        }

        private boolean claimRecorded(int stepIndex, String commandId, String recordedAttemptId) {
            return attemptId.equals(recordedAttemptId) && claim(stepIndex, commandId);
        }

        private String attemptId() {
            if (!consumed.get()) {
                throw new IllegalStateException("Command retry admission has not been consumed");
            }
            return attemptId;
        }

        private void requireConsumed() {
            if (!consumed.get()) {
                throw new IllegalStateException(
                    "Deliberate Command retry did not encounter logical effect " + targetCommandId
                        + " at step " + targetStepIndex);
            }
        }
    }
}
