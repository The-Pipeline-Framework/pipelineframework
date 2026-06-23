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

    @Test
    void resolvePreservesFanOutThroughOneToOneTerminalSteps() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_MANY", "ONE_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolve());
    }

    @Test
    void resolveTreatsFanInAsSingleTerminalShape() throws Exception {
        Path explicit = tempDir.resolve("pipeline.yaml");
        Files.writeString(explicit, pipelineYaml("ONE_TO_MANY", "MANY_TO_ONE"));
        System.setProperty("pipeline.config", explicit.toString());

        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();

        assertEquals(ExecutionResultShape.SINGLE, resolver.resolve());
    }

    private static String pipelineYaml(String terminalCardinality) {
        return pipelineYaml("ONE_TO_ONE", terminalCardinality);
    }

    private static String pipelineYaml(String firstCardinality, String terminalCardinality) {
        return """
            basePackage: org.example
            transport: GRPC
            steps:
              - name: Process Input
                cardinality: %s
                input: org.example.Input
                output: org.example.Intermediate
              - name: Process Output
                cardinality: %s
                input: org.example.Intermediate
                output: org.example.Output
            """.formatted(firstCardinality, terminalCardinality);
    }
}
