package org.pipelineframework.connector.llm;

import java.util.Objects;
import java.util.Map;
import java.util.Optional;

import org.pipelineframework.config.pipeline.PipelineYamlCallable;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Strict compiled callable selection; none of these fields are model-authored. */
public record LlmCallableConfiguration(
    String using,
    String operation,
    String kind,
    int operationVersion,
    String input,
    Optional<Map<String, String>> trustedArguments
) {
    public LlmCallableConfiguration {
        trustedArguments = Objects.requireNonNull(
            trustedArguments, "trusted argument mappings must not be null").map(Map::copyOf);
        PipelineYamlCallable checked = new PipelineYamlCallable(
            "validated", using, operation, PipelineYamlCallable.parseKind(kind), operationVersion, input,
            trustedArguments.orElseGet(Map::of), java.util.Optional.empty(), "RETURN_RECORDED", Map.of(), Map.of());
        using = checked.using();
        operation = checked.operation();
        kind = checked.kindToken();
        operationVersion = checked.operationVersion();
        input = checked.input();
        trustedArguments = Optional.of(checked.trustedArguments());
    }

    public LlmCallableConfiguration(
        String using,
        String operation,
        String kind,
        int operationVersion,
        String input,
        Map<String, String> trustedArguments
    ) {
        this(using, operation, kind, operationVersion, input, Optional.of(
            Objects.requireNonNull(trustedArguments, "trusted argument mappings must not be null")));
    }

    public ConnectorOperationKind operationKind() {
        return PipelineYamlCallable.parseKind(kind);
    }

    public Map<String, String> trustedArgumentMappings() {
        return trustedArguments.orElseGet(Map::of);
    }
}
