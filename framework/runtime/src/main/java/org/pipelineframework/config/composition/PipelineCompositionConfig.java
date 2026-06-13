package org.pipelineframework.config.composition;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Top-level typed pipeline composition manifest.
 *
 * @param version composition manifest version
 * @param name logical composition name
 * @param pipelines pipeline YAML files included in the composition
 */
public record PipelineCompositionConfig(
    int version,
    String name,
    List<PipelineCompositionPipeline> pipelines
) {
    public PipelineCompositionConfig {
        if (version != 1) {
            throw new IllegalArgumentException("pipeline composition version must be 1");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("pipeline composition name must not be blank");
        }
        if (pipelines == null || pipelines.isEmpty()) {
            throw new IllegalArgumentException("pipeline composition must declare at least one pipeline");
        }
        name = name.trim();
        pipelines = List.copyOf(pipelines);

        Set<String> ids = new LinkedHashSet<>();
        List<String> duplicates = new ArrayList<>();
        for (PipelineCompositionPipeline pipeline : pipelines) {
            if (!ids.add(pipeline.id())) {
                duplicates.add(pipeline.id());
            }
        }
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("Duplicate pipeline id(s) in composition: " + duplicates);
        }
    }
}
