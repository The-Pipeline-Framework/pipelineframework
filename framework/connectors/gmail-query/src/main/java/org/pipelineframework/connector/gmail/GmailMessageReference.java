package org.pipelineframework.connector.gmail;

import java.util.Objects;

/** Stable Gmail list/search result reference. */
public record GmailMessageReference(String id, String threadId) {
    public GmailMessageReference {
        id = Objects.requireNonNull(id, "Gmail message ID must not be null");
        threadId = Objects.requireNonNull(threadId, "Gmail thread ID must not be null");
    }
}
