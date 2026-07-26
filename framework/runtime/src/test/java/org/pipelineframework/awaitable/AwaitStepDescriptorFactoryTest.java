package org.pipelineframework.awaitable;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitStepDescriptorFactoryTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearPipelineConfigProperty() {
        System.clearProperty("pipeline.config");
    }

    @Test
    void descriptorUsesExplicitPipelineConfigFileNotSiblingDefault() throws Exception {
        Files.writeString(tempDir.resolve("pipeline.yaml"), pipelineYaml("kafka", """
                    request:
                      topic: default.requests
                    response:
                      topic: default.responses
            """));
        Path explicit = tempDir.resolve("pipeline.container-sqs.yaml");
        Files.writeString(explicit, pipelineYaml("sqs", """
                    request:
                      queueUrl: http://localhost:4566/000000000000/requests
                    response:
                      queueUrl: http://localhost:4566/000000000000/responses
            """));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor = factory.descriptor(
                "ProcessAwaitPaymentProviderService",
                "org.example.PaymentRecord",
                "org.example.PaymentStatus").await().indefinitely();

            assertEquals("sqs", descriptor.transportType());
            @SuppressWarnings("unchecked")
            var request = assertInstanceOf(java.util.Map.class, descriptor.transportConfig().get("request"));
            assertEquals(
                "http://localhost:4566/000000000000/requests",
                request.get("queueUrl"));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void legacyDescriptorOverloadRejectsConflictingCachedTransportIdentity() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("kafka", """
                    request:
                      topic: payments.requests
                    response:
                      topic: payments.responses
            """));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            factory.descriptor(
                "ProcessAwaitPaymentProviderService",
                "org.example.PaymentRecord",
                "org.example.PaymentStatus",
                "org.example.transport.PaymentRecord",
                "org.example.transport.PaymentStatus").await().indefinitely();

            var conflictingDescriptor = assertDoesNotThrow(() -> factory.descriptor(
                "ProcessAwaitPaymentProviderService",
                "org.example.PaymentRecord",
                "org.example.PaymentStatus",
                "org.example.transport.PaymentRecord",
                "org.example.transport.DifferentPaymentStatus"));
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> conflictingDescriptor.await().indefinitely());

            assertTrue(failure.getMessage().contains("Conflicting await descriptor identities"));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void rebuildsLegacyAwaitDescriptorFromDeclaredStepIdForASeparateRuntime() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("interaction-api", ""));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor =
                factory.descriptorByStepIdNow("ProcessAwaitPaymentProviderService");

            assertEquals("org.example.PaymentRecord", descriptor.inputType());
            assertEquals("org.example.PaymentStatus", descriptor.outputType());
            assertEquals(descriptor.inputType(), descriptor.transportInputType());
            assertEquals(descriptor.outputType(), descriptor.transportOutputType());
            assertEquals("interaction-api", descriptor.transportType());
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void rejectsUndeclaredAwaitStepIdDuringDurableContractReconstruction() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("interaction-api", ""));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> factory.descriptorByStepIdNow("ProcessUnknownAwaitService"));

            assertTrue(failure.getMessage().contains("No await YAML step found"));
        } finally {
            factory.shutdown();
        }
    }

    private static String pipelineYaml(String transportType, String transportConfig) {
        return """
            basePackage: org.example
            transport: GRPC
            steps:
              - name: Await Payment Provider
                kind: await
                cardinality: ONE_TO_ONE
                input: org.example.PaymentRecord
                output: org.example.PaymentStatus
                timeout: PT5M
                await:
                  correlation:
                    strategy: signedResumeToken
                  transport:
                    type: %s
            %s
            """.formatted(transportType, transportConfig);
    }
}
