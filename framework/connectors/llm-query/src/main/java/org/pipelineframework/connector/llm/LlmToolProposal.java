package org.pipelineframework.connector.llm;

import java.util.Objects;

/** Untrusted model observation naming one exposed alias and its serialized arguments. */
public record LlmToolProposal(String alias, String argumentsJson) {
    public LlmToolProposal {
        alias = Objects.requireNonNull(alias, "model tool alias must not be null").trim();
        argumentsJson = Objects.requireNonNull(argumentsJson, "model tool arguments must not be null").trim();
    }
}
