package org.pipelineframework.objectingest;

import java.util.List;
import java.util.Optional;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;

/**
 * Runtime-neutral provider SPI for listing and optionally loading object source items.
 */
public interface ObjectSourceProvider {

    String providerName();

    /**
     * Lists object source items up to {@code limit}.
     *
     * @return listed items, never {@code null}; return an empty list when no items are available
     */
    List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit);

    default Optional<String> readText(PipelineObjectSourceConfig source, ObjectSourceItem item, long maxBytes) {
        return Optional.empty();
    }
}
