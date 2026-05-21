package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitStepDescriptorTest {

    @Test
    void constructsWithAllFields() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
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
        assertEquals("single", descriptor.dispatchMode());
    }

    @Test
    void acceptsManyToManyPerItemDispatch() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "await-payment-provider",
            "com.example.PaymentRecord",
            "com.example.PaymentStatus",
            "MANY_TO_MANY",
            "per-item",
            Duration.ofMinutes(5),
            "signedResumeToken",
            "kafka",
            Map.of(),
            List.of("csvId"));

        assertEquals("MANY_TO_MANY", descriptor.cardinality());
        assertEquals("per-item", descriptor.dispatchMode());
    }

    @Test
    void defaultsManyToManyDispatchToPerItem() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "await-payment-provider",
            "com.example.PaymentRecord",
            "com.example.PaymentStatus",
            "MANY_TO_MANY",
            null,
            Duration.ofMinutes(5),
            "signedResumeToken",
            "kafka",
            Map.of(),
            List.of());

        assertEquals("per-item", descriptor.dispatchMode());
    }

    @Test
    void defaultsCorrelationStrategyToInteractionIdWhenNull() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), null, "webhook", null, null);

        assertEquals("interactionId", descriptor.correlationStrategy());
    }

    @Test
    void defaultsCorrelationStrategyToInteractionIdWhenBlank() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "  ", "webhook", null, null);

        assertEquals("interactionId", descriptor.correlationStrategy());
    }

    @Test
    void normalizesNullTransportConfigToEmptyMap() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(Map.of(), descriptor.transportConfig());
    }

    @Test
    void makesImmutableCopyOfTransportConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "https://example.com");

        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", config, null);

        config.put("extra", "value"); // mutate original
        assertEquals(1, descriptor.transportConfig().size());
        assertThrows(UnsupportedOperationException.class, () -> descriptor.transportConfig().put("k", "v"));
    }

    @Test
    void normalizesNullIdempotencyKeyFieldsToEmptyList() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(List.of(), descriptor.idempotencyKeyFields());
    }

    @Test
    void makesImmutableCopyOfIdempotencyKeyFields() {
        List<String> fields = new ArrayList<>();
        fields.add("orderId");

        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, fields);

        fields.add("customerId"); // mutate original
        assertEquals(1, descriptor.idempotencyKeyFields().size());
        assertThrows(UnsupportedOperationException.class, () -> descriptor.idempotencyKeyFields().add("x"));
    }

    @Test
    void rejectsBlankStepId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "  ", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNullStepId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            null, "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankInputType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "  ",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNullTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            null, "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsNegativeTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(-1), "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsZeroDurationTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ZERO, "interactionId", "webhook", null, null));
    }

    @Test
    void rejectsBlankTransportType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "", null, null));
    }

    @Test
    void rejectsNullTransportType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", null, null, null));
    }

    @Test
    void rejectsManyToManyWithoutPerItemDispatch() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id",
            "com.example.Input",
            "com.example.Output",
            "MANY_TO_MANY",
            "single",
            Duration.ofMinutes(5),
            "interactionId",
            "kafka",
            null,
            null));
    }

    @Test
    void rejectsPerItemDispatchForUnaryAwait() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "step-id",
            "com.example.Input",
            "com.example.Output",
            "ONE_TO_ONE",
            "per-item",
            Duration.ofMinutes(5),
            "interactionId",
            "kafka",
            null,
            null));
    }
}
