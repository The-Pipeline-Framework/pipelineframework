package org.pipelineframework.awaitable.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.config.pipeline.PipelineJson;

class KafkaAwaitCompletionConsumerTest {

    @Test
    void consumesCompletionEnvelopeThroughCoordinator() throws Exception {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record(), false)));
        KafkaAwaitCompletionConsumer consumer = new KafkaAwaitCompletionConsumer(executionService);
        String body = PipelineJson.mapper().writeValueAsString(new KafkaAwaitCompletionEnvelope(
            "tenant-1",
            "interaction-1",
            null,
            "resume-token",
            "completion-1",
            Map.of("decision", "approved"),
            "fraud-service"));
        AtomicReference<Boolean> acked = new AtomicReference<>(false);

        consumer.consume(message(body, acked, new AtomicReference<>()))
            .toCompletableFuture()
            .orTimeout(5, TimeUnit.SECONDS)
            .join();

        assertEquals(Boolean.TRUE, acked.get());
        ArgumentCaptor<AwaitCompletionCommand> captor = ArgumentCaptor.forClass(AwaitCompletionCommand.class);
        verify(executionService).completeAwaitInteraction(captor.capture());
        AwaitCompletionCommand command = captor.getValue();
        assertEquals("tenant-1", command.tenantId());
        assertEquals("interaction-1", command.interactionId());
        assertEquals("resume-token", command.resumeToken());
        assertEquals("completion-1", command.idempotencyKey());
        assertEquals("fraud-service", command.actor());
    }

    @Test
    void invalidEnvelopeNacksMessage() {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        KafkaAwaitCompletionConsumer consumer = new KafkaAwaitCompletionConsumer(executionService);
        AtomicReference<Throwable> nacked = new AtomicReference<>();

        consumer.consume(message("not-json", new AtomicReference<>(), nacked))
            .toCompletableFuture()
            .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
            .join();

        assertNotNull(nacked.get());
    }

    @Test
    void coordinatorFailureNacksMessage() {
        PipelineExecutionService executionService = mock(PipelineExecutionService.class);
        when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("stale")));
        KafkaAwaitCompletionConsumer consumer = new KafkaAwaitCompletionConsumer(executionService);
        AtomicReference<Throwable> nacked = new AtomicReference<>();

        consumer.consume(message("""
            {
              "tenantId": "tenant-1",
              "interactionId": "interaction-1",
              "idempotencyKey": "completion-1",
              "responsePayload": {"decision": "approved"}
            }
            """, new AtomicReference<>(), nacked))
            .toCompletableFuture()
            .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
            .join();

        assertNotNull(nacked.get());
    }

    private static Message<String> message(
        String payload,
        AtomicReference<Boolean> acked,
        AtomicReference<Throwable> nacked) {
        return Message.of(
            payload,
            () -> {
                acked.set(true);
                return CompletableFuture.completedFuture(null);
            },
            failure -> {
                nacked.set(failure);
                return CompletableFuture.completedFuture(null);
            });
    }

    private static AwaitInteractionRecord record() {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "FraudCheck",
            1,
            "com.example.Decision",
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.COMPLETED,
            Map.of("orderId", "o-1"),
            Map.of("decision", "approved"),
            "fraud-service",
            null,
            null,
            "kafka",
            Map.of(),
            System.currentTimeMillis() + Duration.ofMinutes(5).toMillis(),
            1_000L,
            2_000L,
            99_999L);
    }
}
