package org.pipelineframework.connector.gmail;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Deterministic connector-owned page returned by list and search operations. */
public record GmailMessagePage(
    List<GmailMessageReference> messages,
    Optional<String> nextPageToken,
    long resultSizeEstimate
) {
    public GmailMessagePage {
        messages = List.copyOf(Objects.requireNonNull(messages, "Gmail messages must not be null"));
        nextPageToken = GmailListMessagesRequest.nonBlank(nextPageToken, "Gmail next page token");
        if (resultSizeEstimate < 0) {
            throw new IllegalArgumentException("Gmail result size estimate must not be negative");
        }
    }
}
