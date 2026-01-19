package org.pipelineframework.processor.phase;

import java.util.List;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PipelineTargetResolutionPhase.
 */
public class PipelineTargetResolutionPhaseTest {

    @Test
    public void testPipelineTargetResolutionPhaseCreation() {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        
        assertNotNull(phase);
        assertEquals("Pipeline Target Resolution Phase", phase.name());
    }

    @Test
    public void testResolveClientRole() {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        
        // Test the resolveClientRole functionality by creating a model and verifying role changes
        // Since resolveClientRole is private, we'll test it indirectly through the execute method
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        
        // Create a sample model with PLUGIN_SERVER role
        PipelineStepModel model = new PipelineStepModel(
            "TestService",
            "TestService",
            "com.example.service",
            ClassName.get("com.example.service", "TestService"),
            new TypeMapping(null, null, false),
            new TypeMapping(null, null, false),
            StreamingShape.UNARY_UNARY,
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            org.pipelineframework.processor.ir.ExecutionMode.DEFAULT,
            DeploymentRole.PLUGIN_SERVER,
            false,
            null
        );
        
        context.setStepModels(List.of(model));
        
        // Execute the phase (this will call resolveClientRole internally when client targets are allowed)
        // Note: This test is limited because we can't directly trigger the client role resolution
        // without orchestrator elements in the context
        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    public void testResolveTransportTargets() {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        
        // Test GRPC with client targets allowed
        java.util.Set<GenerationTarget> grpcWithClients = java.util.Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP);
        java.util.Set<GenerationTarget> grpcWithoutClients = java.util.Set.of(GenerationTarget.GRPC_SERVICE);
        
        java.util.Set<GenerationTarget> restWithClients = java.util.Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP);
        java.util.Set<GenerationTarget> restWithoutClients = java.util.Set.of(GenerationTarget.REST_RESOURCE);
        
        // These tests would require reflection to access the private methods
        // For now, we just ensure the phase can be instantiated and executed
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        assertDoesNotThrow(() -> phase.execute(context));
    }
}