package org.pipelineframework.examples.agentproof.domain;

public record AgentState(String effectScope, String nextEffectKey, String evidence, String phase) {
}
