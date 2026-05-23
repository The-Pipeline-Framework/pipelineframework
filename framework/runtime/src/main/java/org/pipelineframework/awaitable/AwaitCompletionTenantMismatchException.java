package org.pipelineframework.awaitable;

/**
 * Completion admitted for a tenant different from the durable interaction tenant.
 */
public class AwaitCompletionTenantMismatchException extends IllegalArgumentException implements AwaitCompletionAdmissionFailure {

    public AwaitCompletionTenantMismatchException(String message) {
        super(message);
    }
}
