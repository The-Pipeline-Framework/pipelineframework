package org.pipelineframework.connector.gmail;

import java.util.Objects;
import java.util.Optional;

/** Static list/search limits, kept separate from the invocation payload. */
public record GmailListMessagesConfiguration(Optional<Long> maxResults, boolean includeSpamTrash) {
    public GmailListMessagesConfiguration {
        maxResults = Objects.requireNonNull(maxResults, "Gmail maximum results must not be null");
        maxResults.ifPresent(value -> {
            if (value < 1 || value > 500) {
                throw new IllegalArgumentException("Gmail maximum results must be between 1 and 500");
            }
        });
    }
}
