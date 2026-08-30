package org.pipelineframework.connector.gmail;

import java.util.Objects;
import java.util.Optional;

/** Dynamic pagination input for {@code list.messages}; it contains no account selection. */
public record GmailListMessagesRequest(Optional<String> pageToken) {
    public GmailListMessagesRequest {
        pageToken = nonBlank(pageToken, "Gmail page token");
    }

    static Optional<String> nonBlank(Optional<String> value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        value.ifPresent(item -> {
            if (item.isBlank() || !item.equals(item.strip())) {
                throw new IllegalArgumentException(label + " must not be blank or have surrounding whitespace");
            }
        });
        return value;
    }
}
