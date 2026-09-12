/* Copyright (c) 2026 Mariano Barcia. Licensed under the Apache License, Version 2.0. */
package org.pipelineframework.config.template;

/** Classification of a change on one compatibility surface. */
public enum PipelineCompatibilityImpact {
    UNCHANGED,
    COMPATIBLE,
    WIDENING,
    BREAKING,
    LOSSY,
    REQUIRES_REVIEW;

    public boolean breaking() {
        return this == BREAKING || this == LOSSY;
    }
}
