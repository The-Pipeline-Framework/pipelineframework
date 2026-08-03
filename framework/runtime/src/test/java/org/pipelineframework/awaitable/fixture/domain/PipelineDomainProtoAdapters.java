package org.pipelineframework.awaitable.fixture.domain;

import org.pipelineframework.awaitable.fixture.grpc.PipelineTypes;

public final class PipelineDomainProtoAdapters {

    private PipelineDomainProtoAdapters() {
    }

    public static PipelineTypes.PaymentRecord toProto(PaymentRecord value) {
        return new PipelineTypes.PaymentRecord(value.id());
    }

    public static PaymentStatus fromProto(PipelineTypes.PaymentStatus value) {
        return new PaymentStatus(value.status());
    }
}
