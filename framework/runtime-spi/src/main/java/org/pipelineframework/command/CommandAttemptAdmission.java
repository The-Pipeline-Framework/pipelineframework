package org.pipelineframework.command;

import java.util.Optional;

/** Store-authoritative admission for appending one deliberate Command attempt. */
public record CommandAttemptAdmission(
    CommandAttemptPurpose purpose,
    Optional<String> reason
) {
    public CommandAttemptAdmission {
        if (purpose == null || purpose == CommandAttemptPurpose.INITIAL) {
            throw new IllegalArgumentException("deliberate attempt purpose must be RETRY or REISSUE");
        }
        reason = reason == null ? Optional.empty() : reason.map(String::trim).filter(value -> !value.isEmpty());
        if (purpose == CommandAttemptPurpose.REISSUE && reason.isEmpty()) {
            throw new IllegalArgumentException("Command reissue reason must not be blank");
        }
    }

    public static CommandAttemptAdmission retry() {
        return new CommandAttemptAdmission(CommandAttemptPurpose.RETRY, Optional.empty());
    }

    public static CommandAttemptAdmission reissue(String reason) {
        return new CommandAttemptAdmission(CommandAttemptPurpose.REISSUE, Optional.ofNullable(reason));
    }
}
