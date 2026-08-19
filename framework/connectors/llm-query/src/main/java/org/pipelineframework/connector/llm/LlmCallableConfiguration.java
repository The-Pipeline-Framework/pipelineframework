package org.pipelineframework.connector.llm;

import java.util.Objects;

import org.pipelineframework.config.pipeline.PipelineYamlCallable;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Strict compiled callable selection; none of these fields are model-authored. */
public record LlmCallableConfiguration(
    String using,
    String operation,
    String kind,
    int operationVersion,
    String input
) {
    public LlmCallableConfiguration {
        PipelineYamlCallable checked = new PipelineYamlCallable(
            "validated", using, operation, PipelineYamlCallable.parseKind(kind), operationVersion, input);
        using = checked.using();
        operation = checked.operation();
        kind = checked.kindToken();
        operationVersion = checked.operationVersion();
        input = checked.input();
    }

    public ConnectorOperationKind operationKind() {
        return PipelineYamlCallable.parseKind(kind);
    }
}
