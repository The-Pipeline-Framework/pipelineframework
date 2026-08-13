package org.pipelineframework.command;

import java.util.Objects;
import org.pipelineframework.step.NonRetryableException;

/**
 * Sanitized failure exposed to the pipeline for a typed native command outcome.
 */
public final class CommandOutcomeException extends NonRetryableException {
    private final CommandEffectStatus status;
    private final String outcomeCode;

    public CommandOutcomeException(CommandEffectStatus status, String outcomeCode) {
        super(message(status, outcomeCode));
        this.status = Objects.requireNonNull(status, "command outcome status must not be null");
        this.outcomeCode = requireCode(outcomeCode);
    }

    public CommandEffectStatus status() {
        return status;
    }

    public String outcomeCode() {
        return outcomeCode;
    }

    private static String message(CommandEffectStatus status, String outcomeCode) {
        return "command outcome " + Objects.requireNonNull(status, "command outcome status must not be null")
            .name().toLowerCase(java.util.Locale.ROOT) + ": " + requireCode(outcomeCode);
    }

    private static String requireCode(String outcomeCode) {
        if (outcomeCode == null || outcomeCode.isBlank()) {
            throw new IllegalArgumentException("command outcome code must not be blank");
        }
        return outcomeCode;
    }
}
