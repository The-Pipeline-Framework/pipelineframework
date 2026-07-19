package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class SqsAwaitTransportAdapterTest {

    @Test
    void supportsLiveAwaitWindowOnlyWhenTheLocalPollerOwnsTheResponseQueue() {
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        assertTrue(adapter(mock(SqsClient.class), () -> new SqsAwaitTransportAdapter.SqsLiveAwaitWindowConfig(
            true,
            Optional.of("http://sqs.local/responses"))).supportsLiveAwaitWindow(descriptor));
        assertFalse(adapter(mock(SqsClient.class), () -> new SqsAwaitTransportAdapter.SqsLiveAwaitWindowConfig(
            false,
            Optional.of("http://sqs.local/responses"))).supportsLiveAwaitWindow(descriptor));
        assertFalse(adapter(mock(SqsClient.class), () -> new SqsAwaitTransportAdapter.SqsLiveAwaitWindowConfig(
            true,
            Optional.of("http://sqs.local/other-responses"))).supportsLiveAwaitWindow(descriptor));
    }

    @Test
    void normalizesRequestQueueForAdmissionScope() {
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        assertEquals("sqs://http://sqs.local/requests", adapter(mock(SqsClient.class))
            .admissionEndpoint(descriptor).orElseThrow());
    }

    @Test
    void dispatchPublishesFrameworkEnvelope() throws Exception {
        SqsClient client = mock(SqsClient.class);
        SqsAwaitTransportAdapter adapter = adapter(client);
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        var result = adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
            descriptor,
            interaction(),
            Map.of("paymentId", "p-1"))).await().atMost(Duration.ofSeconds(5));

        verify(client).sendMessage(argThat((SendMessageRequest request) -> {
            try {
                JsonNode body = PipelineJson.mapper().readTree(request.messageBody());
                return request.queueUrl().equals("http://sqs.local/requests")
                    && body.get("tenantId").asText().equals("tenant-1")
                    && body.get("executionId").asText().equals("exec-1")
                    && body.get("interactionId").asText().equals("interaction-1")
                    && body.get("correlationId").asText().equals("corr-1")
                    && body.get("stepId").asText().equals("PaymentProvider")
                    && body.hasNonNull("resumeToken")
                    && body.get("requestPayload").get("paymentId").asText().equals("p-1");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
        assertEquals("sqs", result.metadata().get("adapter"));
        assertEquals("http://sqs.local/requests", result.metadata().get("requestQueueUrl"));
        assertEquals("http://sqs.local/responses", result.metadata().get("responseQueueUrl"));
    }

    @Test
    void dispatchRejectsMissingRequestQueueUrl() {
        SqsAwaitTransportAdapter adapter = adapter(mock(SqsClient.class));
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of(),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertTrue(exception.getMessage().contains("request.queueUrl"));
    }

    @Test
    void dispatchRejectsMissingResponseQueueUrl() {
        SqsAwaitTransportAdapter adapter = adapter(mock(SqsClient.class));
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests"),
            "response", Map.of()));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertTrue(exception.getMessage().contains("response.queueUrl"));
    }

    @Test
    void dispatchRejectsFifoQueueUrl() {
        SqsAwaitTransportAdapter adapter = adapter(mock(SqsClient.class));
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests.fifo"),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertTrue(exception.getMessage().contains("standard queues only"));
    }

    @Test
    void dispatchRejectsNormalizedFifoQueueUrl() {
        SqsAwaitTransportAdapter adapter = adapter(mock(SqsClient.class));
        AwaitStepDescriptor descriptor = descriptor(Map.of(
            "request", Map.of("queueUrl", "http://sqs.local/requests.fifo?ignored=true#fragment"),
            "response", Map.of("queueUrl", "http://sqs.local/responses")));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction(),
                Map.of())).await().atMost(Duration.ofSeconds(5)));

        assertTrue(exception.getMessage().contains("standard queues only"));
    }

    private static SqsAwaitTransportAdapter adapter(SqsClient client) {
        SqsAwaitTransportAdapter adapter = new SqsAwaitTransportAdapter(client, config());
        adapter.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        return adapter;
    }

    private static SqsAwaitTransportAdapter adapter(
        SqsClient client,
        java.util.function.Supplier<SqsAwaitTransportAdapter.SqsLiveAwaitWindowConfig> liveAwaitWindowConfig
    ) {
        SqsAwaitTransportAdapter adapter = new SqsAwaitTransportAdapter(client, config(), liveAwaitWindowConfig);
        adapter.resumeTokenService = new AwaitResumeTokenService("secret-value-for-tests");
        return adapter;
    }

    private static PipelineOrchestratorConfig config() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.SqsConfig sqs = mock(PipelineOrchestratorConfig.SqsConfig.class);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }

    private static AwaitStepDescriptor descriptor(Map<String, Object> config) {
        return new AwaitStepDescriptor(
            "PaymentProvider",
            "com.example.PaymentRecord",
            "com.example.PaymentStatus",
            Duration.ofMinutes(10),
            "signedResumeToken",
            "sqs",
            config,
            java.util.List.of("paymentId"));
    }

    private static AwaitInteractionRecord interaction() {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "PaymentProvider",
            1,
            "com.example.PaymentStatus",
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.DISPATCHING,
            Map.of("paymentId", "p-1"),
            null,
            null,
            null,
            null,
            "sqs",
            Map.of(),
            99_999_999_999L,
            1_000L,
            2_000L,
            99_999L);
    }
}
