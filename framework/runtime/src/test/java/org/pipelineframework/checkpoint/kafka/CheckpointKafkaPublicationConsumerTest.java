package org.pipelineframework.checkpoint.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.ws.rs.NotFoundException;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.checkpoint.CheckpointPublicationAdmissionService;
import org.pipelineframework.checkpoint.CheckpointPublicationEnvelope;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class CheckpointKafkaPublicationConsumerTest {

    @Test
    void consumesCheckpointEnvelopeThroughAdmissionService() throws Exception {
        CheckpointPublicationAdmissionService admissionService = mock(CheckpointPublicationAdmissionService.class);
        when(admissionService.admit(any(CheckpointPublicationRequest.class), eq("tenant-1"), eq("idem-1")))
            .thenReturn(Uni.createFrom().item(new RunAsyncAcceptedDto("exec-1", false, "/status/exec-1", 1L)));
        KafkaCheckpointPublicationConsumer consumer = new KafkaCheckpointPublicationConsumer(admissionService);
        AtomicReference<Boolean> acked = new AtomicReference<>(false);

        consumer.consume(message(body("orders-ready"), acked, new AtomicReference<>()))
            .toCompletableFuture()
            .orTimeout(5, TimeUnit.SECONDS)
            .join();

        assertEquals(Boolean.TRUE, acked.get());
        ArgumentCaptor<CheckpointPublicationRequest> captor = ArgumentCaptor.forClass(CheckpointPublicationRequest.class);
        verify(admissionService).admit(captor.capture(), eq("tenant-1"), eq("idem-1"));
        assertEquals("orders-ready", captor.getValue().publication());
        assertEquals("o-1", captor.getValue().payload().get("orderId").asText());
    }

    @Test
    void invalidEnvelopeNacksMessage() {
        KafkaCheckpointPublicationConsumer consumer = new KafkaCheckpointPublicationConsumer(
            mock(CheckpointPublicationAdmissionService.class));
        AtomicReference<Throwable> nacked = new AtomicReference<>();

        consumer.consume(message("not-json", new AtomicReference<>(), nacked))
            .toCompletableFuture()
            .orTimeout(5, TimeUnit.SECONDS)
            .join();

        assertNotNull(nacked.get());
    }

    @Test
    void missingSubscriberAcksMessage() throws Exception {
        CheckpointPublicationAdmissionService admissionService = mock(CheckpointPublicationAdmissionService.class);
        when(admissionService.admit(any(CheckpointPublicationRequest.class), eq("tenant-1"), eq("idem-1")))
            .thenReturn(Uni.createFrom().failure(new NotFoundException("missing publication")));
        KafkaCheckpointPublicationConsumer consumer = new KafkaCheckpointPublicationConsumer(admissionService);
        AtomicReference<Boolean> acked = new AtomicReference<>(false);
        AtomicReference<Throwable> nacked = new AtomicReference<>();

        consumer.consume(message(body("orders-ready"), acked, nacked))
            .toCompletableFuture()
            .orTimeout(5, TimeUnit.SECONDS)
            .join();

        assertEquals(Boolean.TRUE, acked.get());
        assertEquals(null, nacked.get());
    }

    @Test
    void admissionFailureNacksMessage() throws Exception {
        CheckpointPublicationAdmissionService admissionService = mock(CheckpointPublicationAdmissionService.class);
        when(admissionService.admit(any(CheckpointPublicationRequest.class), eq("tenant-1"), eq("idem-1")))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("transient")));
        KafkaCheckpointPublicationConsumer consumer = new KafkaCheckpointPublicationConsumer(admissionService);
        AtomicReference<Throwable> nacked = new AtomicReference<>();

        consumer.consume(message(body("orders-ready"), new AtomicReference<>(), nacked))
            .toCompletableFuture()
            .orTimeout(5, TimeUnit.SECONDS)
            .join();

        assertNotNull(nacked.get());
    }

    private static String body(String publication) throws Exception {
        return PipelineJson.mapper().writeValueAsString(new CheckpointPublicationEnvelope(
            publication,
            "tenant-1",
            "idem-1",
            PipelineJson.mapper().valueToTree(new PublishedOrder("o-1")),
            1L));
    }

    private static Message<String> message(
        String payload,
        AtomicReference<Boolean> acked,
        AtomicReference<Throwable> nacked
    ) {
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

    private record PublishedOrder(String orderId) {
    }
}
