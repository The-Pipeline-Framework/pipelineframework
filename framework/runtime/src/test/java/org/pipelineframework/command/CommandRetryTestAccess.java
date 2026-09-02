package org.pipelineframework.command;

import java.util.Optional;

/** Test-only access to package-private Command retry mechanics. */
public final class CommandRetryTestAccess {
    private static final ThreadLocal<CommandReexecutionScope.AdmissionHandle> HANDLE = new ThreadLocal<>();

    private CommandRetryTestAccess() {
    }

    public static void install(String commandId, String admissionKey) {
        HANDLE.set(CommandReexecutionScope.installRetry(commandId, admissionKey));
    }

    public static void installReissue(String commandId, String admissionKey, String reason) {
        HANDLE.set(CommandReexecutionScope.installReissue(commandId, admissionKey, reason));
    }

    public static Optional<String> claimAttempt(String commandId) {
        return CommandReexecutionScope.claimAttempt(commandId, commandId)
            .map(CommandReexecutionScope.Claim::attemptId);
    }

    public static Optional<AttemptClaim> claimAttempt(String commandId, String currentOccurrenceId) {
        return CommandReexecutionScope.claimAttempt(commandId, currentOccurrenceId)
            .map(claim -> new AttemptClaim(
                claim.attemptId(), claim.occurrenceId(), claim.admission().purpose(), claim.admission().reason()));
    }

    public static void requireConsumed() {
        HANDLE.get().requireConsumed();
    }

    public static void clear() {
        CommandReexecutionScope.clear();
        HANDLE.remove();
    }

    public static CommandRetryableEffectException retryableFailure(String commandId, Throwable cause) {
        return CommandRetryableEffectException.at(commandId, cause);
    }

    public record AttemptClaim(
        String attemptId,
        String occurrenceId,
        CommandAttemptPurpose purpose,
        Optional<String> reason
    ) {
    }
}
