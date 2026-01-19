package org.pipelineframework.processor.phase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PipelineSemanticAnalysisPhase.
 */
public class PipelineSemanticAnalysisPhaseTest {

    @Test
    public void testPipelineSemanticAnalysisPhaseCreation() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        assertNotNull(phase);
        assertEquals("Pipeline Semantic Analysis Phase", phase.name());
    }

    @Test
    public void testStreamingShapeMethods() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Test different cardinalities
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_STREAMING, 
                     phase.streamingShape("EXPANSION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_UNARY, 
                     phase.streamingShape("REDUCTION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_STREAMING, 
                     phase.streamingShape("MANY_TO_MANY"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY, 
                     phase.streamingShape("OTHER"));
    }

    @Test
    public void testIsStreamingInputCardinality() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        assertTrue(phase.isStreamingInputCardinality("REDUCTION"));
        assertTrue(phase.isStreamingInputCardinality("MANY_TO_MANY"));
        assertFalse(phase.isStreamingInputCardinality("EXPANSION"));
        assertFalse(phase.isStreamingInputCardinality("OTHER"));
    }

    @Test
    public void testApplyCardinalityToStreaming() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Test EXPANSION turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", false));
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", true));
        
        // Test MANY_TO_MANY turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", false));
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", true));
        
        // Test REDUCTION turns streaming off
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", false));
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", true));
        
        // Test other cardinalities preserve current state
        assertFalse(phase.applyCardinalityToStreaming("OTHER", false));
        assertTrue(phase.applyCardinalityToStreaming("OTHER", true));
    }

    @Test
    public void testIsCacheAspect() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Create a mock aspect model for testing
        org.pipelineframework.processor.ir.PipelineAspectModel cacheAspect = 
            new org.pipelineframework.processor.ir.PipelineAspectModel(
                "cache", 
                org.pipelineframework.processor.ir.AspectScope.GLOBAL, 
                org.pipelineframework.processor.ir.AspectPosition.BEFORE_STEP, 
                java.util.Map.of()
            );
        
        org.pipelineframework.processor.ir.PipelineAspectModel otherAspect = 
            new org.pipelineframework.processor.ir.PipelineAspectModel(
                "other", 
                org.pipelineframework.processor.ir.AspectScope.GLOBAL, 
                org.pipelineframework.processor.ir.AspectPosition.BEFORE_STEP, 
                java.util.Map.of()
            );
        
        assertTrue(phase.isCacheAspect(cacheAspect));
        assertFalse(phase.isCacheAspect(otherAspect));
    }
}