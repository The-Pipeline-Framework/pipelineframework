package org.pipelineframework.config.composition;

/**
 * One pipeline entry in a composition manifest.
 *
 * @param id stable pipeline id within the composition
 * @param path path to the pipeline YAML, resolved relative to the composition manifest
 */
public record PipelineCompositionPipeline(String id, String path) {
    public PipelineCompositionPipeline {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("pipeline composition pipeline id must not be blank");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("pipeline composition pipeline path must not be blank");
        }
        id = id.trim();
        path = path.trim();
    }
}
