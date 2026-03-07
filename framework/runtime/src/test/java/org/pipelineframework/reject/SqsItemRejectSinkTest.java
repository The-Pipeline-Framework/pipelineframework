package org.pipelineframework.reject;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsItemRejectSinkTest {

    @Test
    void providerNameIsSqs() {
        SqsItemRejectSink sink = new SqsItemRejectSink();
        assertEquals("sqs", sink.providerName());
    }

    @Test
    void durableFlagIsTrue() {
        SqsItemRejectSink sink = new SqsItemRejectSink();
        assertTrue(sink.durable());
    }

    @Test
    void startupValidationRequiresQueueUrl() {
        ItemRejectConfig config = mockConfig(Optional.empty());
        SqsItemRejectSink sink = new SqsItemRejectSink(null, config);

        var validationError = sink.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("queue-url"));
    }

    @Test
    void publishSendsMessageToConfiguredQueue() {
        SqsClient client = mock(SqsClient.class);
        ItemRejectConfig config = mockConfig(Optional.of("https://sqs.local/123/reject"));
        SqsItemRejectSink sink = new SqsItemRejectSink(client, config);

        sink.publish(sampleEnvelope()).await().atMost(Duration.ofSeconds(5));

        verify(client).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void publishSerializesEnvelopeAsJson() {
        SqsClient client = mock(SqsClient.class);
        ItemRejectConfig config = mockConfig(Optional.of("https://sqs.local/123/reject"));
        SqsItemRejectSink sink = new SqsItemRejectSink(client, config);

        sink.publish(sampleEnvelope()).await().atMost(Duration.ofSeconds(5));

        verify(client).sendMessage(argThat((SendMessageRequest request) -> {
            String body = request.messageBody();
            return body.contains("tenant-a")
                && body.contains("exec-1")
                && body.contains("java.lang.RuntimeException")
                && body.contains("fingerprint-1");
        }));
    }

    @Test
    void publishThrowsWhenQueueUrlNotConfigured() {
        SqsClient client = mock(SqsClient.class);
        ItemRejectConfig config = mockConfig(Optional.empty());
        SqsItemRejectSink sink = new SqsItemRejectSink(client, config);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> sink.publish(sampleEnvelope()).await().atMost(Duration.ofSeconds(5)));

        assertTrue(error.getMessage().contains("queue-url"));
        verify(client, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void startupValidationRejectsFifoQueueUrl() {
        ItemRejectConfig config = mockConfig(Optional.of("https://sqs.local/123/reject.fifo"));
        SqsItemRejectSink sink = new SqsItemRejectSink(null, config);

        var validationError = sink.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("FIFO"));
    }

    private static ItemRejectEnvelope sampleEnvelope() {
        return new ItemRejectEnvelope(
            "tenant-a",
            "exec-1",
            "corr-1",
            "idem-1",
            "none",
            "com.example.Step",
            "Step",
            "ITEM",
            0,
            0,
            3,
            1,
            RuntimeException.class.getName(),
            "boom",
            System.currentTimeMillis(),
            "fingerprint-1",
            null,
            null);
    }

    private static ItemRejectConfig mockConfig(Optional<String> queueUrl) {
        ItemRejectConfig config = mock(ItemRejectConfig.class);
        ItemRejectConfig.SqsConfig sqsConfig = mock(ItemRejectConfig.SqsConfig.class);
        when(config.sqs()).thenReturn(sqsConfig);
        when(config.provider()).thenReturn("sqs");
        when(config.memoryCapacity()).thenReturn(16);
        when(config.includePayload()).thenReturn(false);
        when(config.publishFailurePolicy()).thenReturn(ItemRejectFailurePolicy.CONTINUE);
        when(config.strictStartup()).thenReturn(true);
        when(sqsConfig.queueUrl()).thenReturn(queueUrl);
        when(sqsConfig.region()).thenReturn(Optional.empty());
        when(sqsConfig.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }
}
