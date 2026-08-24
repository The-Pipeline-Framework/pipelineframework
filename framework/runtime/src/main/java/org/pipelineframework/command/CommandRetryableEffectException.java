package org.pipelineframework.command;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Internal failure marker retaining the exact logical effect that failed retryably. */
public final class CommandRetryableEffectException extends RuntimeException {
    private final String commandId;

    private CommandRetryableEffectException(String commandId, Throwable cause) {
        super(cause == null ? null : cause.getMessage(), cause);
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        this.commandId = commandId;
    }

    static CommandRetryableEffectException at(String commandId, Throwable cause) {
        Objects.requireNonNull(cause, "cause must not be null");
        return find(cause).orElseGet(() -> new CommandRetryableEffectException(commandId, cause));
    }

    static Throwable mark(String commandId, Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        if (find(failure).isEmpty()) {
            failure.addSuppressed(new CommandRetryableEffectException(commandId, null));
        }
        return failure;
    }

    public String commandId() {
        return commandId;
    }

    public static Optional<CommandRetryableEffectException> find(Throwable failure) {
        return find(failure, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    }

    private static Optional<CommandRetryableEffectException> find(Throwable failure, Set<Throwable> visited) {
        Throwable current = failure;
        while (current != null && visited.add(current)) {
            if (current.getClass() == CommandRetryableEffectException.class) {
                return Optional.of((CommandRetryableEffectException) current);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                Optional<CommandRetryableEffectException> marker = find(suppressed, visited);
                if (marker.isPresent()) {
                    return marker;
                }
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return Optional.empty();
    }
}
