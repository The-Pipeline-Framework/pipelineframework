package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CommandStepDescriptorFactoryTest {
    @TempDir Path tempDir;
    private CommandStepDescriptorFactory factory;

    @AfterEach
    void clearConfiguration() {
        System.clearProperty("pipeline.config");
        if (factory != null) factory.shutdown();
    }

    @Test
    void resolvesCommandDeclaredInsideNamedPipeline() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            version: 3
            basePackage: com.example
            transport: LOCAL
            connectors:
              store:
                provider: proof.store
                version: 1
            types:
              Input: { fields: [[id, string]] }
              Output: { fields: [[id, string]] }
            pipelines:
              child:
                input: Input
                output: Output
                steps:
                  - name: Nested command
                    kind: command
                    cardinality: ONE_TO_ONE
                    using: store
                    operation: write
                    operationVersion: 1
                    input: Input
                    output: Output
                    commandIdGenerator: com.example.StableId
                    duplicatePolicy: RETURN_RECORDED
                    java: { input: com.example.Input, output: com.example.Output }
            steps:
              - { name: Call child, pipeline: child, cardinality: ONE_TO_ONE, input: Input, output: Output }
            """);
        System.setProperty("pipeline.config", config.toString());
        factory = new CommandStepDescriptorFactory();

        CommandDescriptor descriptor = factory.descriptor(
            "ProcessNestedCommandService", "fallback", "com.example.Input", "com.example.Output", "fallback.Id")
            .await().indefinitely();

        assertEquals("native-binding:store/write", descriptor.command());
        assertEquals("com.example.StableId", descriptor.commandIdGenerator());
        assertEquals("proof.store", descriptor.nativeSelector().orElseThrow().operationIdentity().providerId().value());
        assertEquals("write", descriptor.nativeSelector().orElseThrow().operationIdentity().operationId());
    }
}
