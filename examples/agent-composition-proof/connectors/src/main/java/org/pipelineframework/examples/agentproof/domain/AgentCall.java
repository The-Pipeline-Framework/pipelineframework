package org.pipelineframework.examples.agentproof.domain;

/** Portable, inert operation proposal selected by the model query. */
public record AgentCall(String binding, String operation, String argumentsJson) {
}
