package org.pipelineframework.query;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;

/**
 * Runtime descriptor for a generated query client step.
 */
public final class QueryStepDescriptor {
    private final String stepId;
    private final String queryId;
    private final String connector;
    private final String version;
    private final String inputType;
    private final String outputType;
    private final String cardinality;
    private final List<String> keyFields;
    private final Optional<PipelineYamlJpaQuery> jpa;
    private final Optional<NativeQuerySelector> nativeSelector;
    private final Map<String, Object> config;

    public QueryStepDescriptor(
        String stepId,
        String queryId,
        String connector,
        String version,
        String inputType,
        String outputType,
        String cardinality,
        List<String> keyFields,
        PipelineYamlJpaQuery jpa
    ) {
        this(stepId, queryId, connector, version, inputType, outputType, cardinality, keyFields,
            Optional.of(Objects.requireNonNull(jpa, "jpa query config must not be null")), Optional.empty(), Map.of());
    }

    private QueryStepDescriptor(
        String stepId,
        String queryId,
        String connector,
        String version,
        String inputType,
        String outputType,
        String cardinality,
        List<String> keyFields,
        Optional<PipelineYamlJpaQuery> jpa,
        Optional<NativeQuerySelector> nativeSelector,
        Map<String, Object> config
    ) {
        this.stepId = requireText(stepId, "stepId");
        this.queryId = requireText(queryId, "queryId");
        this.connector = requireText(connector, "connector");
        this.version = version == null || version.isBlank() ? "v1" : version;
        this.inputType = requireText(inputType, "inputType");
        this.outputType = requireText(outputType, "outputType");
        this.cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality;
        if (!"ONE_TO_ONE".equalsIgnoreCase(this.cardinality)) {
            throw new IllegalArgumentException("Query step '" + stepId + "' supports only ONE_TO_ONE cardinality in v1");
        }
        this.keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        this.jpa = Objects.requireNonNull(jpa, "jpa query config must not be null");
        this.nativeSelector = Objects.requireNonNull(nativeSelector, "native query selector must not be null");
        if (this.jpa.isPresent() == this.nativeSelector.isPresent()) {
            throw new IllegalArgumentException("query descriptor must declare exactly one of jpa or native selector");
        }
        this.config = Map.copyOf(Objects.requireNonNull(config, "query operation config must not be null"));
    }

    public static QueryStepDescriptor nativeQuery(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        NativeQuerySelector selector,
        Map<String, Object> config
    ) {
        NativeQuerySelector checked = Objects.requireNonNull(selector, "native query selector must not be null");
        return new QueryStepDescriptor(
            stepId,
            "native-binding:" + checked.binding().value() + "/" + checked.operationIdentity().operationId(),
            "native",
            "v" + checked.operationIdentity().majorVersion(),
            inputType,
            outputType,
            cardinality,
            List.of(),
            Optional.empty(),
            Optional.of(checked),
            config);
    }

    public String stepId() {
        return stepId;
    }

    public String queryId() {
        return queryId;
    }

    public String connector() {
        return connector;
    }

    public String version() {
        return version;
    }

    public String inputType() {
        return inputType;
    }

    public String outputType() {
        return outputType;
    }

    public String cardinality() {
        return cardinality;
    }

    public List<String> keyFields() {
        return keyFields;
    }

    public PipelineYamlJpaQuery jpa() {
        return jpa.orElseThrow(() -> new IllegalStateException("native Query descriptor does not contain JPA configuration"));
    }

    public Optional<NativeQuerySelector> nativeSelector() {
        return nativeSelector;
    }

    public Map<String, Object> config() {
        return config;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
