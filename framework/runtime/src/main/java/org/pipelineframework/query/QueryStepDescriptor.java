package org.pipelineframework.query;

import java.util.List;
import java.util.Map;

/**
 * Runtime descriptor for a generated query client step.
 */
public record QueryStepDescriptor(
    String stepId,
    String queryId,
    String connector,
    String version,
    String inputType,
    String outputType,
    String cardinality,
    List<String> keyFields,
    Map<String, Object> config
) {
    public QueryStepDescriptor {
        requireText(stepId, "stepId");
        requireText(queryId, "queryId");
        requireText(connector, "connector");
        version = version == null || version.isBlank() ? "v1" : version;
        requireText(inputType, "inputType");
        requireText(outputType, "outputType");
        cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality;
        if (!"ONE_TO_ONE".equalsIgnoreCase(cardinality)) {
            throw new IllegalArgumentException("Query step '" + stepId + "' supports only ONE_TO_ONE cardinality in v1");
        }
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
