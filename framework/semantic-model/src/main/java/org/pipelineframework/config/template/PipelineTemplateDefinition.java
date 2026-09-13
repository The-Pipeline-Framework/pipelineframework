package org.pipelineframework.config.template;

import java.util.List;

/**
 * A locally authored compile-time pipeline definition fragment.
 *
 * <p>The entry key in {@link PipelineTemplateConfig#pipelines()} supplies its logical identity.
 * This fragment intentionally has no deployment or source-location identity, leaving packaged
 * definition linking to enter through the same logical-reference seam later.</p>
 */
public record PipelineTemplateDefinition(
    String inputContract,
    String outputContract,
    List<PipelineTemplateStep> steps
) {
    public PipelineTemplateDefinition {
        if (inputContract == null || inputContract.isBlank()) {
            throw new IllegalArgumentException("inputContract must not be blank");
        }
        if (outputContract == null || outputContract.isBlank()) {
            throw new IllegalArgumentException("outputContract must not be blank");
        }
        inputContract = inputContract.trim();
        outputContract = outputContract.trim();
        steps = steps == null ? List.of() : List.copyOf(steps);
    }
}
