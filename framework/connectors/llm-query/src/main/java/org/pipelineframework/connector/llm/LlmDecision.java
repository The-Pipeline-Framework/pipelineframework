package org.pipelineframework.connector.llm;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.QueryObservation;

/** One inert model proposal together with optional provider-reported observation metadata. */
public record LlmDecision(
    LlmToolProposal proposal,
    Optional<QueryObservation> observation
) {
    public LlmDecision {
        proposal = Objects.requireNonNull(proposal, "LLM tool proposal must not be null");
        observation = Objects.requireNonNull(observation, "LLM observation must not be null");
    }

    public LlmDecision(LlmToolProposal proposal) {
        this(proposal, Optional.empty());
    }
}
