package org.pipelineframework.examples.agentproof.domain;

import java.util.Objects;

public record RecordArguments(String action) {
    public RecordArguments {
        action = Objects.requireNonNull(action, "record action must not be null");
    }
}
