package org.pipelineframework.config.pipeline;

import java.util.List;

/**
 * Capture settings for a query step.
 */
public record PipelineYamlQueryCapture(List<String> keyFields) {
    public PipelineYamlQueryCapture {
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
    }
}
