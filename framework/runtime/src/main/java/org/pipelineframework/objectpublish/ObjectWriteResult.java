package org.pipelineframework.objectpublish;

import java.time.Instant;

import org.pipelineframework.repository.PayloadReference;

/**
 * Result returned by an object target provider after writing a payload.
 */
public record ObjectWriteResult(
    PayloadReference reference,
    long bytes,
    String checksum,
    Instant writtenAt
) {
    public ObjectWriteResult {
        if (bytes < 0) {
            throw new IllegalArgumentException("object write result bytes must be >= 0");
        }
        writtenAt = writtenAt == null ? Instant.EPOCH : writtenAt;
    }
}
