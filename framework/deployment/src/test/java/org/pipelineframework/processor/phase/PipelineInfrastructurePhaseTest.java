package org.pipelineframework.processor.phase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the PipelineInfrastructurePhase.
 */
public class PipelineInfrastructurePhaseTest {

    @Test
    public void testPipelineInfrastructurePhaseCreation() {
        PipelineInfrastructurePhase phase = new PipelineInfrastructurePhase();
        
        assertNotNull(phase);
        assertEquals("Pipeline Infrastructure Phase", phase.name());
    }
}