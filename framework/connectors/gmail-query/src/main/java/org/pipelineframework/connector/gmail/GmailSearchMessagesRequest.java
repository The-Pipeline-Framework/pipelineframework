package org.pipelineframework.connector.gmail;

import java.util.Objects;
import java.util.Optional;

/** Gmail search expression and pagination input; account selection remains host-owned. */
public record GmailSearchMessagesRequest(String query, Optional<String> pageToken) {
    public GmailSearchMessagesRequest {
        query = Objects.requireNonNull(query, "Gmail search query must not be null");
        if (query.isBlank() || !query.equals(query.strip())) {
            throw new IllegalArgumentException("Gmail search query must not be blank or have surrounding whitespace");
        }
        pageToken = GmailListMessagesRequest.nonBlank(pageToken, "Gmail page token");
    }
}
