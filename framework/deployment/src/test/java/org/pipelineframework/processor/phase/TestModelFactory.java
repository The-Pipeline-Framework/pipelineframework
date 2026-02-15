package org.pipelineframework.processor.phase;

import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.ir.*;

/**
 * Factory for creating test PipelineStepModel instances.
 */
public class TestModelFactory {
    
    /**
     * Creates a test PipelineStepModel with the given name.
     *
     * @param name the name for the service
     * @return a new PipelineStepModel for testing
     */
    public static PipelineStepModel createTestModel(String name) {
        return new PipelineStepModel(
            name, name, "com.example.service", ClassName.get("com.example.service", name),
            new TypeMapping(null, null, false), new TypeMapping(null, null, false),
            StreamingShape.UNARY_UNARY, Set.of(), ExecutionMode.DEFAULT,
            DeploymentRole.PIPELINE_SERVER, false, null);
    }
}