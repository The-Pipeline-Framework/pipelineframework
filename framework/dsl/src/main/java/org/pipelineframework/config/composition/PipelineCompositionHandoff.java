package org.pipelineframework.config.composition;

/**
 * Derived typed handoff from one pipeline checkpoint publication to one subscribed pipeline.
 *
 * @param publication checkpoint publication name
 * @param producerPipelineId producer pipeline id
 * @param consumerPipelineId consumer pipeline id
 * @param outputTypeName producer terminal output type name
 * @param inputTypeName consumer entry input type name
 */
public record PipelineCompositionHandoff(
    String publication,
    String producerPipelineId,
    String consumerPipelineId,
    String outputTypeName,
    String inputTypeName
) {
    public PipelineCompositionHandoff {
        if (publication == null || publication.isBlank()) {
            throw new IllegalArgumentException("composition handoff publication must not be blank");
        }
        if (producerPipelineId == null || producerPipelineId.isBlank()) {
            throw new IllegalArgumentException("composition handoff producerPipelineId must not be blank");
        }
        if (consumerPipelineId == null || consumerPipelineId.isBlank()) {
            throw new IllegalArgumentException("composition handoff consumerPipelineId must not be blank");
        }
        if (outputTypeName == null || outputTypeName.isBlank()) {
            throw new IllegalArgumentException("composition handoff outputTypeName must not be blank");
        }
        if (inputTypeName == null || inputTypeName.isBlank()) {
            throw new IllegalArgumentException("composition handoff inputTypeName must not be blank");
        }
        publication = publication.trim();
        producerPipelineId = producerPipelineId.trim();
        consumerPipelineId = consumerPipelineId.trim();
        outputTypeName = outputTypeName.trim();
        inputTypeName = inputTypeName.trim();
    }
}
