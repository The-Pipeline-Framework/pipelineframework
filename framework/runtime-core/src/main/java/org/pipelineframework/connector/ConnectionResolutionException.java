package org.pipelineframework.connector;

/**
 * Safe connector-facing failure raised when a host cannot resolve an authenticated connection.
 * Messages must never contain credentials, tokens, or provider SDK response bodies.
 */
public final class ConnectionResolutionException extends RuntimeException {
    public ConnectionResolutionException(String message) {
        super(message);
    }

    public ConnectionResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
