package org.pipelineframework.connector.gmail;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Read-only connector-owned projection of a Gmail message. */
public record GmailMessage(
    String id,
    String threadId,
    List<String> labelIds,
    Optional<String> snippet,
    Optional<Long> internalDateEpochMillis,
    List<GmailMessageHeader> headers,
    Optional<String> bodyData
) {
    public GmailMessage {
        id = Objects.requireNonNull(id, "Gmail message ID must not be null");
        threadId = Objects.requireNonNull(threadId, "Gmail thread ID must not be null");
        labelIds = List.copyOf(Objects.requireNonNull(labelIds, "Gmail label IDs must not be null"));
        snippet = Objects.requireNonNull(snippet, "Gmail snippet must not be null");
        internalDateEpochMillis = Objects.requireNonNull(
            internalDateEpochMillis, "Gmail internal date must not be null");
        headers = List.copyOf(Objects.requireNonNull(headers, "Gmail headers must not be null"));
        bodyData = Objects.requireNonNull(bodyData, "Gmail body data must not be null");
    }
}
