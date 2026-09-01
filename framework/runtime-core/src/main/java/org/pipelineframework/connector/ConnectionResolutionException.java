package org.pipelineframework.connector;

/**
 * Safe connector-facing failure raised when a host cannot resolve an authenticated connection.
 * Messages must never contain credentials, tokens, or provider SDK response bodies.
 */
public final class ConnectionResolutionException extends RuntimeException {
    private final Kind kind;

    public ConnectionResolutionException(String message) {
        this(Kind.CONFIGURATION, message);
    }

    public ConnectionResolutionException(String message, Throwable cause) {
        this(Kind.CONFIGURATION, message, cause);
    }

    public ConnectionResolutionException(Kind kind, String message) {
        super(message);
        this.kind = java.util.Objects.requireNonNull(kind, "connection resolution failure kind must not be null");
    }

    public ConnectionResolutionException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = java.util.Objects.requireNonNull(kind, "connection resolution failure kind must not be null");
    }

    public Kind kind() {
        return kind;
    }

    public enum Kind {
        AUTHENTICATION_REQUIRED,
        TEMPORARILY_UNAVAILABLE,
        CONFIGURATION
    }
}
