package org.pipelineframework.processor.block;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Effective compiler input and imported-definition provenance. */
public record ImportedPipelineSources(
    Path configPath,
    List<ImportedPipelineDefinition> definitions,
    boolean temporary
) implements AutoCloseable {
    public ImportedPipelineSources {
        if (configPath == null) {
            throw new IllegalArgumentException("configPath must not be null");
        }
        definitions = definitions == null ? List.of() : List.copyOf(definitions);
    }

    @Override
    public void close() throws IOException {
        if (temporary) {
            Files.deleteIfExists(configPath);
        }
    }
}
