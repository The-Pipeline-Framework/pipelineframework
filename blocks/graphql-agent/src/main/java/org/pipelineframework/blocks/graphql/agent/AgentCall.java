package org.pipelineframework.blocks.graphql.agent;

/** Inert model proposal materialised by the existing LLM Query contract. */
public record AgentCall(String binding, String operation, String argumentsJson, String contextJson) {
}
