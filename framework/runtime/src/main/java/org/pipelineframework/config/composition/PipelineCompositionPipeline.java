package org.pipelineframework.config.composition;

import java.util.regex.Pattern;

/**
 * One pipeline entry in a composition manifest.
 *
 * @param id stable pipeline id within the composition
 * @param path path to the pipeline YAML, resolved relative to the composition manifest
 */
public record PipelineCompositionPipeline(String id, String path) {
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9._-]*$");

    public PipelineCompositionPipeline {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("pipeline composition pipeline id must not be blank");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("pipeline composition pipeline path must not be blank");
        }
        id = id.trim();
        if (!ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException(
                "pipeline composition pipeline id must match ^[a-zA-Z][a-zA-Z0-9._-]*$: " + id);
        }
        path = path.trim();
    }
}
