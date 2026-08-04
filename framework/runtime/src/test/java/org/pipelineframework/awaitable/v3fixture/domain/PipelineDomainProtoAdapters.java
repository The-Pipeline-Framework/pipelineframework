package org.pipelineframework.awaitable.v3fixture.domain;

import org.pipelineframework.awaitable.v3fixture.grpc.PipelineTypes;

public final class PipelineDomainProtoAdapters {
    private PipelineDomainProtoAdapters() {
    }

    public static PipelineTypes.AwaitInput toProto(AwaitInput value) {
        return new PipelineTypes.AwaitInput(value.value());
    }

    public static AwaitOutput fromProto(PipelineTypes.AwaitOutput value) {
        return new AwaitOutput.Approved(value.value());
    }
}
