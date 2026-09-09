package org.pipelineframework.blocks.graphql.agent;

public sealed interface OperationObservation {
    String discriminator();

    record Result(OperationResultObservation value) implements OperationObservation {
        @Override public String discriminator() { return "result"; }
    }

    record Empty(OperationEmptyObservation value) implements OperationObservation {
        @Override public String discriminator() { return "empty"; }
    }
}
