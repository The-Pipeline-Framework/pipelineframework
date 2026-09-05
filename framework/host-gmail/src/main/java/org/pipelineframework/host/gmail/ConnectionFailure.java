package org.pipelineframework.host.gmail;

/** Bounded diagnostics: never attach provider responses, credentials or exception causes. */
public final class ConnectionFailure extends RuntimeException {
    public enum Reason { UNAVAILABLE, RETRY_LATER, REAUTHORIZE, INVALID_CALLBACK, CONFLICT, STORAGE, FORBIDDEN }
    private final Reason reason;

    public ConnectionFailure(Reason reason) {
        super("gmail-connection-" + reason.name().toLowerCase(java.util.Locale.ROOT));
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
