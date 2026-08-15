package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Signals that a remote transition exceeded the caller's transport deadline without an
 * authoritative disposition from the worker.
 *
 * <p>This is deliberately not evidence that the worker stopped. The coordinator records the
 * execution as outcome-unknown and must not automatically admit a competing attempt.</p>
 */
public final class RemoteTransitionOutcomeUnknownException extends RuntimeException {

    private final String protocol;
    private final String target;
    private final long elapsedMillis;
    private final long deadlineMillis;

    public RemoteTransitionOutcomeUnknownException(
        String protocol,
        String target,
        long elapsedMillis,
        long deadlineMillis,
        Throwable cause
    ) {
        super("Remote " + Objects.requireNonNull(protocol, "protocol")
            + " transition outcome is unknown after " + elapsedMillis + "ms of a " + deadlineMillis
            + "ms request deadline", Objects.requireNonNull(cause, "cause"));
        this.protocol = protocol;
        this.target = Objects.requireNonNull(target, "target");
        this.elapsedMillis = elapsedMillis;
        this.deadlineMillis = deadlineMillis;
    }

    public String protocol() {
        return protocol;
    }

    public String target() {
        return target;
    }

    public long elapsedMillis() {
        return elapsedMillis;
    }

    public long deadlineMillis() {
        return deadlineMillis;
    }
}
