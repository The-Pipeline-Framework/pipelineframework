package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitCompletionDescriptorTest {

    @Test
    void constructsWithAllFields() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step",
            "com.example.ReviewRequest",
            "com.example.ReviewDecision",
            Duration.ofMinutes(10),
            "interactionId",
            "webhook",
            Map.of("url", "https://example.com"),
            List.of("orderId", "customerId"));

        assertEquals("review-step", descriptor.stepId());
        assertEquals("com.example.ReviewRequest", descriptor.inputType());
        assertEquals("com.example.ReviewDecision", descriptor.outputType());
        assertEquals(Duration.ofMinutes(10), descriptor.timeout());
        assertEquals("interactionId", descriptor.correlationStrategy());
        assertEquals("webhook", descriptor.transportType());
        assertEquals("https://example.com", descriptor.transportConfig().get("url"));
        assertEquals(List.of("orderId", "customerId"), descriptor.idempotencyKeyFields());
        assertEquals("ONE_TO_ONE", descriptor.cardinality());
    }

    @Test
    void rejectsRequestAwareCompletionWithoutAProjector() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "review-step", String.class.getName(), String.class.getName(), "ONE_TO_ONE",
            Duration.ofMinutes(10), "interactionId", "interaction-api", Map.of(), List.of(),
            String.class.getName(), String.class.getName(), java.util.function.Function.identity(),
            java.util.function.Function.identity(), "review-projector-v1", null, true));

        assertEquals("request-aware completion requires a completion projector", failure.getMessage());
    }

    @Test
    void rejectsRequestAwareCompletionWithoutAStableProjectorId() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "review-step", String.class.getName(), String.class.getName(), "ONE_TO_ONE",
            Duration.ofMinutes(10), "interactionId", "interaction-api", Map.of(), List.of(),
            String.class.getName(), String.class.getName(), java.util.function.Function.identity(),
            java.util.function.Function.identity(), null,
            (request, completion, metadata) -> completion, true));

        assertEquals("request-aware completion requires a stable completion projector id", failure.getMessage());
    }

    @Test
    void acceptsCardinalitySpecificConstructorWithoutDispatchMode() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "await-payment-provider",
            "com.example.PaymentRecord",
            "com.example.PaymentStatus",
            "MANY_TO_MANY",
            Duration.ofMinutes(5),
            "signedResumeToken",
            "kafka",
            Map.of(),
            List.of("csvId"));

        assertEquals("MANY_TO_MANY", descriptor.cardinality());
    }

    @Test
    void keepsCanonicalAndTransportIdentitiesDistinctWhenExplicitlyConfigured() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "await-payment-provider",
            "com.example.domain.PaymentRecord",
            "com.example.domain.PaymentStatus",
            "ONE_TO_ONE",
            Duration.ofMinutes(5),
            "interactionId",
            "kafka",
            Map.of(),
            List.of(),
            "com.example.grpc.PipelineTypes.PaymentRecord",
            "com.example.grpc.PipelineTypes.PaymentStatus",
            value -> "proto:" + value,
            value -> "domain:" + value);

        assertEquals("com.example.domain.PaymentRecord", descriptor.inputType());
        assertEquals("com.example.domain.PaymentStatus", descriptor.outputType());
        assertEquals("com.example.grpc.PipelineTypes.PaymentRecord", descriptor.transportInputType());
        assertEquals("com.example.grpc.PipelineTypes.PaymentStatus", descriptor.transportOutputType());
        assertEquals("proto:payment", descriptor.inputToTransport().apply("payment"));
        assertEquals("domain:status", descriptor.outputFromTransport().apply("status"));
    }

    @Test
    void defaultsLegacyTransportIdentitiesToCanonicalIdentities() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(descriptor.inputType(), descriptor.transportInputType());
        assertEquals(descriptor.outputType(), descriptor.transportOutputType());
    }

    @Test
    void defaultsCorrelationStrategyToInteractionIdWhenNull() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), null, "webhook", null, null);

        assertEquals("interactionId", descriptor.correlationStrategy());
    }

    @Test
    void defaultsCorrelationStrategyToInteractionIdWhenBlank() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "  ", "webhook", null, null);

        assertEquals("interactionId", descriptor.correlationStrategy());
    }

    @Test
    void normalizesNullTransportConfigToEmptyMap() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(Map.of(), descriptor.transportConfig());
    }

    @Test
    void makesImmutableCopyOfTransportConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "https://example.com");

        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", config, null);

        config.put("extra", "value"); // mutate original
        assertEquals(1, descriptor.transportConfig().size());
        assertThrows(UnsupportedOperationException.class, () -> descriptor.transportConfig().put("k", "v"));
    }

    @Test
    void rejectsNullTransportConfigKeyWithExplicitDiagnostic() {
        Map<String, Object> config = new HashMap<>();
        config.put(null, "value");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new AwaitCompletionDescriptor(
                "review-step", "com.example.Input", "com.example.Output",
                Duration.ofMinutes(5), "interactionId", "webhook", config, List.of()));

        assertEquals("transportConfig contains null key '<null>'", failure.getMessage());
    }

    @Test
    void rejectsNullTransportConfigValueWithKeyDiagnostic() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", null);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new AwaitCompletionDescriptor(
                "review-step", "com.example.Input", "com.example.Output",
                Duration.ofMinutes(5), "interactionId", "webhook", config, List.of()));

        assertEquals("transportConfig value for key 'url' must not be null", failure.getMessage());
    }

    @Test
    void normalizesNullIdempotencyKeyFieldsToEmptyList() {
        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(List.of(), descriptor.idempotencyKeyFields());
    }

    @Test
    void makesImmutableCopyOfIdempotencyKeyFields() {
        List<String> fields = new ArrayList<>();
        fields.add("orderId");

        AwaitCompletionDescriptor descriptor = new AwaitCompletionDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, fields);

        fields.add("customerId"); // mutate original
        assertEquals(1, descriptor.idempotencyKeyFields().size());
        assertThrows(UnsupportedOperationException.class, () -> descriptor.idempotencyKeyFields().add("x"));
    }

    @Test
    void rejectsBlankStepId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "  ", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNullStepId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            null, "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankInputType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "  ",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNullTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            null, "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNegativeTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(-1), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsZeroDurationTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ZERO, "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankTransportType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "", null, null));
    }

    @Test
    void rejectsNullTransportType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitCompletionDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", null, null, null));
    }

}
