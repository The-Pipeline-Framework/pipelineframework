package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

@ExtendWith(MockitoExtension.class)
class TerminalPublicationFlowTest {

    @Mock
    private CheckpointPublicationService checkpointPublicationService;

    @Mock
    private TerminalOutputPublisher terminalOutputPublisher;

    @Mock
    private ObjectPublishCompletionService objectPublishCompletionService;

    @Test
    void checkpointPublicationHappensBeforeTerminalOutputPublication() {
        ExecutionRecord<Object, Object> record = execution();
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        TerminalPublicationFlow runtime = new TerminalPublicationFlow(
            checkpointPublicationService,
            terminalOutputPublisher);
        when(checkpointPublicationService.publishIfConfigured(record, "out-1"))
            .thenReturn(Uni.createFrom().voidItem());
        when(terminalOutputPublisher.publishIfConfigured(result)).thenReturn(Uni.createFrom().voidItem());

        runtime.publishBeforeSuccess(record, result).await().indefinitely();

        InOrder inOrder = inOrder(checkpointPublicationService, terminalOutputPublisher);
        inOrder.verify(checkpointPublicationService).publishIfConfigured(record, "out-1");
        inOrder.verify(terminalOutputPublisher).publishIfConfigured(result);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void remoteEncodedOutputIsDecodedLazilyForObjectPublish() {
        CountingPayloadCodec codec = new CountingPayloadCodec();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(codec, List.of("published-output"));
        ObjectPublishTerminalPublisher publisher = new ObjectPublishTerminalPublisher(
            objectPublishCompletionService,
            codec);
        ArgumentCaptor<Supplier<List<?>>> supplier = ArgumentCaptor.forClass(Supplier.class);
        when(objectPublishCompletionService.publishIfConfigured(supplier.capture()))
            .thenReturn(Uni.createFrom().voidItem());

        publisher.publishIfConfigured(result).await().indefinitely();

        assertEquals(0, codec.decodeCount());
        assertEquals(List.of("published-output"), supplier.getValue().get());
        assertEquals(1, codec.decodeCount());
    }

    @Test
    void alreadyPublishedTerminalOutputSkipsObjectPublish() {
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"), true);
        ObjectPublishTerminalPublisher publisher = new ObjectPublishTerminalPublisher(
            objectPublishCompletionService,
            new JsonTransitionPayloadCodec());

        publisher.publishIfConfigured(result).await().indefinitely();

        verifyNoInteractions(objectPublishCompletionService);
    }

    @Test
    void nullObjectPublishServiceIsNoOp() {
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        ObjectPublishTerminalPublisher publisher = new ObjectPublishTerminalPublisher(
            null,
            new JsonTransitionPayloadCodec());

        publisher.publishIfConfigured(result).await().indefinitely();
    }

    @Test
    void emptyTerminalOutputSkipsCheckpointButStillRunsTerminalPublicationNoOp() {
        ExecutionRecord<Object, Object> record = execution();
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of());
        TerminalPublicationFlow runtime = new TerminalPublicationFlow(
            checkpointPublicationService,
            terminalOutputPublisher);
        when(terminalOutputPublisher.publishIfConfigured(result)).thenReturn(Uni.createFrom().voidItem());

        runtime.publishBeforeSuccess(record, result).await().indefinitely();

        verify(checkpointPublicationService, never()).publishIfConfigured(any(), any());
        verify(terminalOutputPublisher).publishIfConfigured(result);
    }

    @Test
    void terminalPublicationFailurePropagates() {
        ExecutionRecord<Object, Object> record = execution();
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        TerminalPublicationFlow runtime = new TerminalPublicationFlow(
            checkpointPublicationService,
            terminalOutputPublisher);
        IllegalStateException failure = new IllegalStateException("publish failed");
        when(checkpointPublicationService.publishIfConfigured(record, "out-1"))
            .thenReturn(Uni.createFrom().voidItem());
        when(terminalOutputPublisher.publishIfConfigured(result)).thenReturn(Uni.createFrom().failure(failure));

        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.publishBeforeSuccess(record, result).await().indefinitely());

        assertEquals(failure, thrown);
    }

    private static ExecutionRecord<Object, Object> execution() {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "exec-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.RUNNING,
            1L,
            3,
            0,
            "worker",
            0L,
            0L,
            "previous",
            "input",
            null,
            null,
            null,
            null,
            10_000L,
            10_000L,
            86_400L);
    }

    private static final class CountingPayloadCodec implements TransitionPayloadCodec {
        private final JsonTransitionPayloadCodec delegate = new JsonTransitionPayloadCodec();
        private final AtomicInteger decodeCount = new AtomicInteger();

        @Override
        public String encoding() {
            return delegate.encoding();
        }

        @Override
        public SerializedTransitionPayload encode(Object payload) {
            return delegate.encode(payload);
        }

        @Override
        public Object decode(SerializedTransitionPayload payload) {
            decodeCount.incrementAndGet();
            return delegate.decode(payload);
        }

        int decodeCount() {
            return decodeCount.get();
        }
    }
}
