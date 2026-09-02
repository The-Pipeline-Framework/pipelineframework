package org.pipelineframework.command;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pipelineframework.runtime.core.RuntimeAdapters;

/** Internal invocation-scoped authority for one control-plane-admitted Command attempt. */
public final class CommandReexecutionScope {
    private static final String CONTEXT_KEY = CommandReexecutionScope.class.getName() + ".admission";

    private CommandReexecutionScope() {
    }

    public static Snapshot capture() {
        return new Snapshot(RuntimeAdapters.executionContext(CONTEXT_KEY, Admission.class));
    }

    public static AdmissionHandle installRetry(String targetCommandId, String admissionKey) {
        return install(targetCommandId, admissionKey, CommandAttemptAdmission.retry());
    }

    public static AdmissionHandle installReissue(String targetCommandId, String admissionKey, String reason) {
        return install(targetCommandId, admissionKey, CommandAttemptAdmission.reissue(reason));
    }

    private static AdmissionHandle install(
        String targetCommandId,
        String admissionKey,
        CommandAttemptAdmission attemptAdmission
    ) {
        Admission admission = new Admission(targetCommandId, admissionKey, attemptAdmission);
        RuntimeAdapters.setExecutionContext(CONTEXT_KEY, admission);
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

    static Optional<Claim> claimAttempt(String commandId, String currentOccurrenceId) {
        return current()
            .filter(admission -> admission.claim(commandId))
            .map(admission -> admission.claimDetails(currentOccurrenceId));
    }

    static boolean claimRecorded(String commandId, String attemptId) {
        return current()
            .filter(admission -> admission.claimRecorded(commandId, attemptId))
            .isPresent();
    }

    private static Optional<Admission> current() {
        return Optional.ofNullable(RuntimeAdapters.executionContext(CONTEXT_KEY, Admission.class));
    }

    record Claim(String attemptId, String occurrenceId, CommandAttemptAdmission admission) {
    }

    public static final class Snapshot {
        private final Admission admission;

        private Snapshot(Admission admission) {
            this.admission = admission;
        }
    }

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
        private final String targetCommandId;
        private final CommandAttemptAdmission attemptAdmission;
        private final String attemptId;
        private final String reissueOccurrenceId;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private Admission(
            String targetCommandId,
            String admissionKey,
            CommandAttemptAdmission attemptAdmission
        ) {
            if (targetCommandId == null || targetCommandId.isBlank()) {
                throw new IllegalArgumentException("targetCommandId must not be blank");
            }
            if (admissionKey == null || admissionKey.isBlank()) {
                throw new IllegalArgumentException("admissionKey must not be blank");
            }
            this.targetCommandId = targetCommandId;
            this.attemptAdmission = Objects.requireNonNull(attemptAdmission, "attemptAdmission must not be null");
            this.attemptId = deterministicId("attempt", admissionKey, targetCommandId, attemptAdmission.purpose());
            this.reissueOccurrenceId = deterministicId(
                "occurrence", admissionKey, targetCommandId, attemptAdmission.purpose());
        }

        private boolean claim(String commandId) {
            return targetCommandId.equals(commandId) && consumed.compareAndSet(false, true);
        }

        private Claim claimDetails(String currentOccurrenceId) {
            String occurrenceId = attemptAdmission.purpose() == CommandAttemptPurpose.RETRY
                ? currentOccurrenceId
                : reissueOccurrenceId;
            return new Claim(attemptId, occurrenceId, attemptAdmission);
        }

        private boolean claimRecorded(String commandId, String recordedAttemptId) {
            return attemptId.equals(recordedAttemptId) && claim(commandId);
        }

        private void requireConsumed() {
            if (!consumed.get()) {
                throw new IllegalStateException(
                    "Deliberate Command " + attemptAdmission.purpose().name().toLowerCase()
                        + " did not encounter logical effect " + targetCommandId);
            }
        }

        private static String deterministicId(
            String prefix,
            String admissionKey,
            String targetCommandId,
            CommandAttemptPurpose purpose
        ) {
            UUID identity = UUID.nameUUIDFromBytes(
                (prefix + "\u0000" + purpose + "\u0000" + admissionKey + "\u0000" + targetCommandId)
                    .getBytes(StandardCharsets.UTF_8));
            return prefix + "-" + identity;
        }
    }
}
