package org.pipelineframework.connector;

import java.util.Objects;
import org.pipelineframework.repository.PayloadReference;

/** Immutable, bounded payload bytes materialized from a portable reference. */
public record MaterializedPayload(
    PayloadReference reference,
    byte[] bytes,
    String contentType,
    String codec,
    String checksum
) {
    public MaterializedPayload {
        reference = Objects.requireNonNull(reference, "materialized payload reference must not be null");
        bytes = Objects.requireNonNull(bytes, "materialized payload bytes must not be null").clone();
    }

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }
}
