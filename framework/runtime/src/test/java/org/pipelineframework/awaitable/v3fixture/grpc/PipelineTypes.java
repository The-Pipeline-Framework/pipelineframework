package org.pipelineframework.awaitable.v3fixture.grpc;

public final class PipelineTypes {
    private PipelineTypes() {
    }

    public record AwaitInput(String value) {
    }

    public record AwaitOutput(String value) {
    }
}
