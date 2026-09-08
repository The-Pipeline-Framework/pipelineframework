package org.pipelineframework.examples.agentproof.domain;

import java.util.Objects;

public record RecordArguments(String action, String effectKey) {
    public RecordArguments {
        action = Objects.requireNonNull(action, "record action must not be null");
        effectKey = Objects.requireNonNull(effectKey, "record effectKey must not be null");
    }
}
