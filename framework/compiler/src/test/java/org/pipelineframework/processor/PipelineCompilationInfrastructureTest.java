package org.pipelineframework.processor;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.phase.ModelExtractionPhase;
import org.pipelineframework.processor.phase.PipelineDiscoveryPhase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the core compilation infrastructure.
 */
public class PipelineCompilationInfrastructureTest {

    @Test
    public void testPipelineCompilationContextCreation() {
        // This test verifies that the core infrastructure classes can be instantiated
        PipelineCompilationContext context = new PipelineCompilationContext(null, org.pipelineframework.processor.Jsr269SourceInventory.empty());

        assertNotNull(context);
        assertNotNull(context.getStepModels());
        assertNotNull(context.getAspectModels());
        assertNotNull(context.getOrchestratorModels());
        assertNotNull(context.getResolvedTargets());
        assertNotNull(context.getRendererBindings());
    }

    @Test
    public void snapshotsCompilerOptionsIntoAnImmutableHostNeutralView() {
        Map<String, String> hostOptions = new LinkedHashMap<>();
        hostOptions.put("pipeline.transport", "REST");

        PipelineCompilerOptions options = new PipelineCompilerOptions(hostOptions);
        hostOptions.put("pipeline.transport", "GRPC");
        hostOptions.put("pipeline.platform", "FUNCTION");

        assertEquals(Optional.of("REST"), options.value("pipeline.transport"));
        assertEquals(Optional.empty(), options.value("pipeline.platform"));
        assertEquals(Map.of("pipeline.transport", "REST"), options.asMap());
        assertThrows(UnsupportedOperationException.class,
            () -> options.asMap().put("pipeline.platform", "COMPUTE"));
    }

    @Test
    public void testPipelineCompilationPhaseImplementation() {
        // Test that our phase implementations follow the interface contract
        List<PipelineCompilationPhase> phases = Arrays.asList(
            new PipelineDiscoveryPhase(),
            new ModelExtractionPhase()
        );

        for (PipelineCompilationPhase phase : phases) {
            assertNotNull(phase.name());
            assertNotEquals("", phase.name().trim(), "Phase name should not be empty");
        }
    }

    @Test
    public void testPipelineCompilerInstantiation() {
        // Test that we can instantiate the compiler with phases
        List<PipelineCompilationPhase> phases = Arrays.asList(
            new PipelineDiscoveryPhase(),
            new ModelExtractionPhase()
        );

        PipelineCompiler compiler = new PipelineCompiler(phases);
        assertNotNull(compiler);
    }
}
