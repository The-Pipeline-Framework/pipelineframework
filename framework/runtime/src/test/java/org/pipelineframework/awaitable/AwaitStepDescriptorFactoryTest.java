package org.pipelineframework.awaitable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
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
    void descriptorByStepIdNowRebuildsV3CanonicalAndTransportIdentities() throws Exception {
        Path explicit = tempDir.resolve("pipeline.v3.yaml");
        Files.writeString(explicit, v3PipelineYaml());
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor = factory.descriptorByStepIdNow("ProcessAwaitPaymentProviderService");

            assertEquals("org.pipelineframework.awaitable.fixture.domain.PaymentRecord", descriptor.inputType());
            assertEquals("org.pipelineframework.awaitable.fixture.domain.PaymentStatus", descriptor.outputType());
            assertEquals("org.pipelineframework.awaitable.fixture.grpc.PipelineTypes$PaymentRecord",
                descriptor.transportInputType());
            assertEquals("org.pipelineframework.awaitable.fixture.grpc.PipelineTypes$PaymentStatus",
                descriptor.transportOutputType());
            assertEquals(new org.pipelineframework.awaitable.fixture.grpc.PipelineTypes.PaymentRecord("record-1"),
                descriptor.inputToTransport().apply(
                    new org.pipelineframework.awaitable.fixture.domain.PaymentRecord("record-1")));
            var transport = new org.pipelineframework.awaitable.fixture.grpc.PipelineTypes.PaymentStatus("APPROVED");
            assertEquals(new org.pipelineframework.awaitable.fixture.domain.PaymentStatus("APPROVED"),
                descriptor.outputFromTransport().apply(transport));
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
    void requestAwareCompletionOverridesActorPayloadTypeAndLoadsPureProjector() throws Exception {
        Path explicit = tempDir.resolve("pipeline-completion.yaml");
        Files.writeString(explicit, """
            basePackage: org.example
            transport: LOCAL
            steps:
              - name: Await Payment Provider
                kind: await
                cardinality: ONE_TO_ONE
                input: java.lang.String
                output: java.lang.String
                timeout: PT5M
                await:
                  correlation:
                    strategy: interactionId
                  completion:
                    type: java.lang.Integer
                    projector: %s
                  transport:
                    type: interaction-api
            """.formatted(PrefixingProjector.class.getName()));
        System.setProperty("pipeline.config", explicit.toString());
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new ClassLoader(null) { });

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor = factory.descriptor(
                "ProcessAwaitPaymentProviderService",
                String.class.getName(),
                String.class.getName()).await().indefinitely();

            assertEquals(Integer.class.getName(), descriptor.transportOutputType());
            assertTrue(descriptor.requestAwareCompletion());
            assertEquals(PrefixingProjector.class.getName(), descriptor.completionProjectorId());
            assertEquals("request:7", descriptor.completionProjector().project(
                "request", 7, new AwaitCompletionMetadata("interaction-1", "alice", java.time.Instant.EPOCH)));
        } finally {
            factory.shutdown();
            Thread.currentThread().setContextClassLoader(previousLoader);
        }
    }

    @Test
    void rebuildsLegacyAwaitDescriptorOffTheCallingThread() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("interaction-api", ""));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AtomicReference<String> resolutionThread = new AtomicReference<>();

            factory.descriptorByStepId("ProcessAwaitPaymentProviderService")
                .invoke(ignored -> resolutionThread.set(Thread.currentThread().getName()))
                .await().indefinitely();

            assertTrue(resolutionThread.get().startsWith("await-descriptor-loader-"));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void rebuildsLegacyAwaitDescriptorUsingGeneratedClientRuntimeTypes() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("interaction-api", "")
            .replace("basePackage: org.example", "basePackage: org.pipelineframework.awaitable.fixture"));
        System.setProperty("pipeline.config", explicit.toString());

        AwaitStepDescriptorFactory factory = new AwaitStepDescriptorFactory();
        try {
            AwaitStepDescriptor descriptor =
                factory.descriptorByStepIdNow("ProcessAwaitPaymentProviderService");

            assertEquals(
                "org.pipelineframework.awaitable.fixture.service.pipeline.LegacyAwaitInput",
                descriptor.inputType());
            assertEquals(
                "org.pipelineframework.awaitable.fixture.service.pipeline.LegacyAwaitOutput",
                descriptor.outputType());
            assertEquals(descriptor.inputType(), descriptor.transportInputType());
            assertEquals(descriptor.outputType(), descriptor.transportOutputType());
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

    public static final class PrefixingProjector
        implements AwaitCompletionProjector<String, Integer, String> {

        @Override
        public String project(String request, Integer completion, AwaitCompletionMetadata metadata) {
            return request + ":" + completion;
        }
    }

    private static String v3PipelineYaml() {
        return """
            version: 3
            appName: await-fixture
            basePackage: org.pipelineframework.awaitable.fixture
            transport: GRPC
            types:
              PaymentRecord:
                fields:
                  - [id, string]
              PaymentStatus:
                fields:
                  - [status, string]
            steps:
              - name: Await Payment Provider
                kind: await
                cardinality: ONE_TO_ONE
                input: PaymentRecord
                output: PaymentStatus
                java:
                  input: org.pipelineframework.awaitable.fixture.domain.PaymentRecord
                  output: org.pipelineframework.awaitable.fixture.domain.PaymentStatus
                timeout: PT5M
                await:
                  correlation:
                    strategy: signedResumeToken
                  transport:
                    type: kafka
                    request:
                      topic: payments.requests
                    response:
                      topic: payments.responses
            """;
    }
}
