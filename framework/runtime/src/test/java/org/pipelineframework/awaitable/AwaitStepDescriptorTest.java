package org.pipelineframework.awaitable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.awaitable.v3fixture.domain.AwaitInput;
import org.pipelineframework.awaitable.v3fixture.domain.AwaitOutput;
import org.pipelineframework.awaitable.v3fixture.grpc.PipelineTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitStepDescriptorTest {

    @TempDir
    Path tempDir;

    @Test
    void legacyGeneratedV3ClientCallRebuildsTheCanonicalProtobufBoundary() throws Exception {
        Path config = writeVersion3FixtureConfig();
        String previous = System.getProperty("pipeline.config");
        System.setProperty("pipeline.config", config.toString());
        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor = factory.descriptor(
                "ProcessAwaitOutputService",
                AwaitInput.class.getName(),
                AwaitOutput.class.getName()).await().indefinitely();

            assertEquals(PipelineTypes.AwaitInput.class.getName(), descriptor.transportInputType());
            assertEquals(PipelineTypes.AwaitOutput.class.getName(), descriptor.transportOutputType());
            assertInstanceOf(
                AwaitOutput.Approved.class,
                descriptor.outputFromTransport().apply(new PipelineTypes.AwaitOutput("approved")));
        } finally {
            factory.shutdown();
            if (previous == null) {
                System.clearProperty("pipeline.config");
            } else {
                System.setProperty("pipeline.config", previous);
            }
        }
    }

    @Test
    void version3TransportOnlyClientRetainsItsIdentityRepresentationBoundary() throws Exception {
        Path config = writeVersion3FixtureConfig();
        String previous = System.getProperty("pipeline.config");
        System.setProperty("pipeline.config", config.toString());
        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor = factory.descriptor(
                "ProcessAwaitOutputService",
                PipelineTypes.AwaitInput.class.getName(),
                PipelineTypes.AwaitOutput.class.getName()).await().indefinitely();

            assertEquals(PipelineTypes.AwaitInput.class.getName(), descriptor.inputType());
            assertEquals(PipelineTypes.AwaitOutput.class.getName(), descriptor.outputType());
            assertEquals(PipelineTypes.AwaitInput.class.getName(), descriptor.transportInputType());
            assertEquals(PipelineTypes.AwaitOutput.class.getName(), descriptor.transportOutputType());
            PipelineTypes.AwaitOutput transportOutput = new PipelineTypes.AwaitOutput("approved");
            assertEquals(transportOutput, descriptor.outputFromTransport().apply(transportOutput));
        } finally {
            factory.shutdown();
            if (previous == null) {
                System.clearProperty("pipeline.config");
            } else {
                System.setProperty("pipeline.config", previous);
            }
        }
    }

    private Path writeVersion3FixtureConfig() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            version: 3
            appName: await-fixture
            basePackage: org.pipelineframework.awaitable.v3fixture
            transport: LOCAL
            types:
              AwaitInput:
                fields:
                  - [value, string]
              AwaitOutput:
                variants:
                  approved: AwaitInput
            steps:
              - name: Await Output
                kind: await
                input: AwaitInput
                output: AwaitOutput
                cardinality: ONE_TO_ONE
                timeout: PT1M
                await:
                  correlation:
                    strategy: generated
                  transport:
                    type: interaction-api
                    config: {}
            """);
        return config;
    }

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
    }

    @Test
    void rejectsRequestAwareCompletionWithoutAProjector() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> new AwaitStepDescriptor(
            "review-step", String.class.getName(), String.class.getName(), "ONE_TO_ONE",
            Duration.ofMinutes(10), "interactionId", "interaction-api", Map.of(), List.of(),
            String.class.getName(), String.class.getName(), java.util.function.Function.identity(),
            java.util.function.Function.identity(), null, true));

        assertEquals("request-aware completion requires a completion projector", failure.getMessage());
    }

    @Test
    void acceptsCardinalitySpecificConstructorWithoutDispatchMode() {
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
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
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
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
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            "review-step", "com.example.Input", "com.example.Output",
            Duration.ofMinutes(5), "interactionId", "webhook", null, null);

        assertEquals(descriptor.inputType(), descriptor.transportInputType());
        assertEquals(descriptor.outputType(), descriptor.transportOutputType());
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

}
