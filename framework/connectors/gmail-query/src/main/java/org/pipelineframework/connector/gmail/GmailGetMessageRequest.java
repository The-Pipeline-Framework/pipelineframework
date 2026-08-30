package org.pipelineframework.connector.gmail;

import java.util.Objects;

/** Dynamic message identity for {@code get.message}; it contains no account selection. */
public record GmailGetMessageRequest(String messageId) {
    public GmailGetMessageRequest {
        messageId = Objects.requireNonNull(messageId, "Gmail message ID must not be null");
        if (messageId.isBlank() || !messageId.equals(messageId.trim())) {
            throw new IllegalArgumentException("Gmail message ID must not be blank or have surrounding whitespace");
        }
    }
}
