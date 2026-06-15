package org.pipelineframework.awaitable;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

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
