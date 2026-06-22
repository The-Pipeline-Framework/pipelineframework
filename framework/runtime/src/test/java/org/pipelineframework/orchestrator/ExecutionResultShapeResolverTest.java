package org.pipelineframework.orchestrator;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExecutionResultShapeResolverTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearPipelineConfigProperty() {
        System.clearProperty("pipeline.config");
    }

    @Test
    void resolveUsesExplicitPipelineConfigFileNotSiblingDefault() throws Exception {
        Files.writeString(tempDir.resolve("pipeline.yaml"), pipelineYaml("ONE_TO_MANY"));
        Path explicit = tempDir.resolve("pipeline.container-sqs.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.SINGLE, resolver.resolve());
    }

    private static String pipelineYaml(String terminalCardinality) {
        return """
            basePackage: org.example
            transport: GRPC
            steps:
              - name: Process Input
                cardinality: ONE_TO_ONE
                input: org.example.Input
                output: org.example.Intermediate
              - name: Process Output
                cardinality: %s
                input: org.example.Intermediate
                output: org.example.Output
            """.formatted(terminalCardinality);
    }
}
