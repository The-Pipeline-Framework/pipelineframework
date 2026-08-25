package org.pipelineframework.command;

import java.util.Optional;

/** Test-only access to package-private Command retry mechanics. */
public final class CommandRetryTestAccess {
    private static final ThreadLocal<CommandRetryExecutionScope.AdmissionHandle> HANDLE = new ThreadLocal<>();

    private CommandRetryTestAccess() {
    }

    public static void install(String commandId, String admissionKey) {
        HANDLE.set(CommandRetryExecutionScope.installRetry(commandId, admissionKey));
    }

    public static Optional<String> claimAttempt(String commandId) {
        return CommandRetryExecutionScope.claimAttempt(commandId);
    }

    public static void requireConsumed() {
        HANDLE.get().requireConsumed();
    }

    public static void clear() {
        CommandRetryExecutionScope.clear();
        HANDLE.remove();
    }

    public static CommandRetryableEffectException retryableFailure(String commandId, Throwable cause) {
        return CommandRetryableEffectException.at(commandId, cause);
    }
}
