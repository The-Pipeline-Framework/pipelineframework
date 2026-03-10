package org.pipelineframework;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineStepConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineStepResolverTest {

    @Test
    void instantiateStepsFromConfigReturnsEmptyWhenNoStepsConfigured() {
        PipelineStepResolver resolver = new PipelineStepResolver();

        java.util.List<Object> steps = resolver.instantiateStepsFromConfig(Map.of());

        assertEquals(0, steps.size());
    }

    @Test
    void instantiateStepsFromConfigFailsWhenStepClassCannotBeResolved() {
        PipelineStepResolver resolver = new PipelineStepResolver();

        Map<String, PipelineStepConfig.StepConfig> configs = new java.util.HashMap<>();
        configs.put("com.example.DoesNotExist", null);

        assertThrows(PipelineExecutionService.PipelineConfigurationException.class,
            () -> resolver.instantiateStepsFromConfig(configs));
    }
}
