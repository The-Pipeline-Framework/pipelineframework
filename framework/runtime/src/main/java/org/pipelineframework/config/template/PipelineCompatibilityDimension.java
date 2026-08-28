/* Copyright (c) 2026 Mariano Barcia. Licensed under the Apache License, Version 2.0. */
package org.pipelineframework.config.template;

/** Independently evaluated compatibility surfaces for a normalized contract change. */
public enum PipelineCompatibilityDimension {
    NORMALIZED_IDL,
    PROTOBUF_WIRE,
    CANONICAL_DATA,
    GENERATED_JAVA_SOURCE
}
