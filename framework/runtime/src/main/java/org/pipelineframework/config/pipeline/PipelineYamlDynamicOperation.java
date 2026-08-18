package org.pipelineframework.config.pipeline;

import java.util.Objects;

/** Compiler-owned binding for runtime selection from one previously exposed callable catalogue. */
public record PipelineYamlDynamicOperation(String from) {
    public PipelineYamlDynamicOperation {
        from = Objects.requireNonNull(from, "dynamic operation source must not be null").trim();
        if (from.isEmpty()) {
            throw new IllegalArgumentException("dynamic operation source must not be blank");
        }
    }
}
