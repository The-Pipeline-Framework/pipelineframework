package org.pipelineframework.processor;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.phase.ModelExtractionPhase;
import org.pipelineframework.processor.phase.PipelineDiscoveryPhase;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the core compilation infrastructure.
 */
public class PipelineCompilationInfrastructureTest {

    @Test
    public void testPipelineCompilationContextCreation() {
        // This test verifies that the core infrastructure classes can be instantiated
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);

        assertNotNull(context);
        assertNotNull(context.getStepModels());
        assertNotNull(context.getAspectModels());
        assertNotNull(context.getOrchestratorModels());
        assertNotNull(context.getResolvedTargets());
        assertNotNull(context.getRendererBindings());
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
