package org.pipelineframework;

import java.util.Objects;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

final class ObjectPublishTerminalPublisher implements TerminalOutputPublisher {

    private final ObjectPublishCompletionService objectPublishCompletionService;
    private final TransitionPayloadCodec payloadCodec;

    ObjectPublishTerminalPublisher(
        ObjectPublishCompletionService objectPublishCompletionService,
        TransitionPayloadCodec payloadCodec) {
        this.objectPublishCompletionService = objectPublishCompletionService;
        this.payloadCodec = payloadCodec;
    }

    @Override
    public Uni<Void> publishIfConfigured(TransitionResultEnvelope result) {
        Objects.requireNonNull(result, "result must not be null");
        if (result.terminalOutputPublished() || objectPublishCompletionService == null) {
            return Uni.createFrom().voidItem();
        }
        return objectPublishCompletionService.publishIfConfigured(() -> result.decodeOutputItems(payloadCodec));
    }
}
