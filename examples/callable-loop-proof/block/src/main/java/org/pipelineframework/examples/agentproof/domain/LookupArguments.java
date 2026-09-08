package org.pipelineframework.examples.agentproof.domain;

import java.util.Objects;

public record LookupArguments(String subject) {
    public LookupArguments {
        subject = Objects.requireNonNull(subject, "lookup subject must not be null");
    }
}
