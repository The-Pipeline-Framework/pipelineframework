package org.pipelineframework.connector.llm;

public record AgentCall(String binding, String operation, String argumentsJson) {
}
