package org.pipelineframework.branching;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineBranchingResourceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineBranchingRegistryDefinitionScopeTest {

    @Test
    void preservesDescriptorsForTheSameRuntimeClassInRootAndChildDefinitions() {
        String runtimeClass = SharedStep.class.getName();
        var root = step("$root", 2, 1, "Root shared", runtimeClass);
        var child = step("inner", 1, 0, "Child shared", runtimeClass);
        var registry = new PipelineBranchingRegistry(
            new PipelineBranchingResourceLoader.BranchingResource(2, List.of(root, child)));

        assertEquals("Root shared", registry.descriptorFor(SharedStep.class).orElseThrow().stepName());
        assertEquals(
            "Child shared",
            registry.descriptorFor("inner", 1, SharedStep.class).orElseThrow().stepName());
    }

    private PipelineBranchingResourceLoader.BranchingStep step(
        String definitionId,
        int terminalStepIndex,
        int index,
        String name,
        String runtimeClass
    ) {
        return new PipelineBranchingResourceLoader.BranchingStep(
            definitionId,
            terminalStepIndex,
            index,
            name,
            runtimeClass,
            Object.class.getName(),
            List.of(Object.class.getName()),
            List.of(Object.class.getName()),
            List.of(),
            List.of(),
            List.of(),
            false);
    }

    static final class SharedStep {
    }
}
