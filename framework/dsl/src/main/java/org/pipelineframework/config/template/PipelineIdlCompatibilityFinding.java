/* Copyright (c) 2026 Mariano Barcia. Licensed under the Apache License, Version 2.0. */
package org.pipelineframework.config.template;

import java.util.Objects;

/** One dimension-specific compatibility consequence. */
public record PipelineIdlCompatibilityFinding(
    String subject,
    PipelineCompatibilityDimension dimension,
    PipelineCompatibilityImpact impact,
    String explanation
) {
    public PipelineIdlCompatibilityFinding {
        Objects.requireNonNull(subject, "subject must not be null");
        Objects.requireNonNull(dimension, "dimension must not be null");
        Objects.requireNonNull(impact, "impact must not be null");
        Objects.requireNonNull(explanation, "explanation must not be null");
    }

    public String diagnostic() {
        return subject + " is " + dimension.name().toLowerCase().replace('_', '-') + " "
            + impact.name().toLowerCase().replace('_', '-') + ": " + explanation;
    }
}
