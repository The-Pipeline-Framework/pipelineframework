package org.pipelineframework.host.oidc;

/** Bounded diagnostics: never attach provider responses, credentials or exception causes. */
public final class ConnectionFailure extends RuntimeException {
    public enum Reason { UNAVAILABLE, RETRY_LATER, UNCERTAIN, REAUTHORIZE, INVALID_CALLBACK, CONFLICT, STORAGE, FORBIDDEN }
    private final Reason reason;

    public ConnectionFailure(Reason reason) {
        super("oidc-connection-" + reason.name().toLowerCase(java.util.Locale.ROOT));
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
