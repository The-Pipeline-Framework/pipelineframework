package org.pipelineframework.query;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.connector.QueryCapabilities;

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
    private final Optional<QueryCapabilities> queryCapabilities;
    private final Optional<Duration> negativeCacheTtl;

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
            Optional.of(Objects.requireNonNull(jpa, "jpa query config must not be null")), Optional.empty(), Map.of(),
            Optional.empty(), Optional.empty());
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
        Map<String, Object> config,
        Optional<QueryCapabilities> queryCapabilities,
        Optional<Duration> negativeCacheTtl
    ) {
        this.stepId = requireText(stepId, "stepId");
        this.queryId = requireText(queryId, "queryId");
        this.connector = requireText(connector, "connector");
        this.version = version == null || version.isBlank() ? "v1" : version;
        this.inputType = requireText(inputType, "inputType");
        this.outputType = requireText(outputType, "outputType");
        this.cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality;
        if (!"ONE_TO_ONE".equalsIgnoreCase(this.cardinality)
            && !"ONE_TO_MANY".equalsIgnoreCase(this.cardinality)) {
            throw new IllegalArgumentException(
                "Query step '" + stepId + "' requires ONE_TO_ONE or ONE_TO_MANY cardinality");
        }
        this.keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        this.jpa = Objects.requireNonNull(jpa, "jpa query config must not be null");
        this.nativeSelector = Objects.requireNonNull(nativeSelector, "native query selector must not be null");
        if (this.jpa.isPresent() == this.nativeSelector.isPresent()) {
            throw new IllegalArgumentException("query descriptor must declare exactly one of jpa or native selector");
        }
        this.config = Map.copyOf(Objects.requireNonNull(config, "query operation config must not be null"));
        this.queryCapabilities = Objects.requireNonNull(queryCapabilities, "query capabilities must not be null");
        this.negativeCacheTtl = Objects.requireNonNull(negativeCacheTtl, "negative cache TTL must not be null");
        if (this.nativeSelector.isPresent() && "ONE_TO_ONE".equalsIgnoreCase(this.cardinality)
            && this.queryCapabilities.isEmpty()) {
            throw new IllegalArgumentException("unary native query descriptors must declare query capabilities");
        }
        if ("ONE_TO_MANY".equalsIgnoreCase(this.cardinality)
            && (this.queryCapabilities.isPresent() || this.negativeCacheTtl.isPresent())) {
            throw new IllegalArgumentException("streaming native query descriptors do not support generic Query cache metadata");
        }
    }

    public static QueryStepDescriptor nativeQuery(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        NativeQuerySelector selector,
        Map<String, Object> config
    ) {
        return nativeQuery(
            stepId,
            inputType,
            outputType,
            cardinality,
            selector,
            config,
            QueryCapabilities.conservative(),
            Optional.empty());
    }

    public static QueryStepDescriptor nativeStreamingQuery(
        String stepId,
        String inputType,
        String outputType,
        NativeQuerySelector selector,
        Map<String, Object> config,
        List<String> keyFields
    ) {
        NativeQuerySelector checked = Objects.requireNonNull(selector, "native query selector must not be null");
        return new QueryStepDescriptor(
            stepId,
            "native-binding:" + checked.binding().value() + "/" + checked.operationIdentity().operationId(),
            "native",
            "v" + checked.operationIdentity().majorVersion(),
            inputType,
            outputType,
            "ONE_TO_MANY",
            keyFields,
            Optional.empty(),
            Optional.of(checked),
            config,
            Optional.empty(),
            Optional.empty());
    }

    public static QueryStepDescriptor nativeQuery(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        NativeQuerySelector selector,
        Map<String, Object> config,
        QueryCapabilities capabilities,
        Optional<Duration> negativeCacheTtl
    ) {
        return nativeQuery(
            stepId,
            inputType,
            outputType,
            cardinality,
            selector,
            config,
            List.of(),
            capabilities,
            negativeCacheTtl);
    }

    public static QueryStepDescriptor nativeQuery(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        NativeQuerySelector selector,
        Map<String, Object> config,
        List<String> keyFields,
        QueryCapabilities capabilities,
        Optional<Duration> negativeCacheTtl
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
            keyFields,
            Optional.empty(),
            Optional.of(checked),
            config,
            Optional.of(Objects.requireNonNull(capabilities, "query capabilities must not be null")),
            Objects.requireNonNull(negativeCacheTtl, "negative cache TTL must not be null"));
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

    public Optional<QueryCapabilities> queryCapabilities() {
        return queryCapabilities;
    }

    public Optional<Duration> negativeCacheTtl() {
        return negativeCacheTtl;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
