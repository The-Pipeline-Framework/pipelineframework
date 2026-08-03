package org.pipelineframework.awaitable.fixture.grpc;

public final class PipelineTypes {

    private PipelineTypes() {
    }

    public record PaymentRecord(String id) {
    }

    public record PaymentStatus(String status) {
    }
}
