package org.pipelineframework.stream;

import java.util.List;
import java.util.Objects;

/** Canonical outputs produced by one bounded read of a provider-owned source. */
public record ResumableSourcePage<O>(List<O> items, OpaqueSourceCheckpoint nextCheckpoint, boolean endOfSource) {
    public ResumableSourcePage {
        items = items == null ? List.of() : List.copyOf(items);
        nextCheckpoint = Objects.requireNonNull(nextCheckpoint, "nextCheckpoint must not be null");
        if (!endOfSource && items.isEmpty()) {
            throw new IllegalArgumentException("A non-terminal resumable source page must contain at least one item");
        }
    }
}
