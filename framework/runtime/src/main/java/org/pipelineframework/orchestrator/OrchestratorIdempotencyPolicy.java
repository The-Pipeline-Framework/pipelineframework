package org.pipelineframework.orchestrator;

/**
 * Idempotency policy for async execution submissions.
 */
public enum OrchestratorIdempotencyPolicy {
    /**
     * Prefer caller key when present and derive deterministic fallback otherwise.
     */
    OPTIONAL_CLIENT_KEY,
    /**
     * Require caller-provided idempotency key.
     */
    CLIENT_KEY_REQUIRED,
    /**
     * Always derive key on server side.
     */
    SERVER_KEY_ONLY
}
