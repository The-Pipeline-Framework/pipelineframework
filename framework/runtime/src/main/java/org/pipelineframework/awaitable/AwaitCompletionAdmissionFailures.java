package org.pipelineframework.awaitable;

/**
 * Helpers for classifying deterministic await completion admission failures.
 */
public final class AwaitCompletionAdmissionFailures {

    private AwaitCompletionAdmissionFailures() {
    }

    public static boolean isDeterministic(Throwable failure) {
        return rootCause(failure) instanceof AwaitCompletionAdmissionFailure;
    }

    public static String reason(Throwable failure) {
        Throwable root = rootCause(failure);
        if (root instanceof AwaitInteractionNotFoundException) {
            return "not_found";
        }
        if (root instanceof AwaitInteractionTerminalException) {
            return "terminal";
        }
        if (root instanceof AwaitResumeTokenRejectedException) {
            return "resume_token_rejected";
        }
        if (root instanceof AwaitCompletionTenantMismatchException) {
            return "tenant_mismatch";
        }
        return "unknown";
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current != null && current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
