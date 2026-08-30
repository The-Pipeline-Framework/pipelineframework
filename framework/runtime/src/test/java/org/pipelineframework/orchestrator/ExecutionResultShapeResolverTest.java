package org.pipelineframework.orchestrator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    void resolveUsesGeneratedContractWhenNoExplicitPipelineConfigExists() {
        PipelineContractDescriptorLoader contractLoader = mock(PipelineContractDescriptorLoader.class);
        when(contractLoader.load()).thenReturn(Optional.of(new PipelineContractDescriptor(
            2,
            "search",
            "contract-v1",
            "hash-v1",
            "COMPUTE",
            "REST",
            "orchestrator-svc",
            false,
            "modular",
            List.of(
                step(0, "ONE_TO_MANY"),
                step(1, "ONE_TO_ONE")),
            PipelineBundleCapabilities.defaults())));
        ExecutionResultShapeResolver resolver = new ExecutionResultShapeResolver();
        resolver.contractLoader = contractLoader;

        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, resolver.resolve());
    }

    private static PipelineBundleStepDescriptor step(int index, String cardinality) {
        return new PipelineBundleStepDescriptor(
            index,
            "Step " + index,
            "internal",
            cardinality,
            "org.example.Input" + index,
            "org.example.Output" + index,
            "org.example.Step" + index,
            "org.example.StepClient" + index,
            "");
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
