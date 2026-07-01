package org.pipelineframework.branching;

import java.util.List;

/**
 * Runtime applicability descriptor for one step in a branch-aware pipeline.
 */
public record StepBranchingDescriptor(
    int index,
    String stepName,
    String runtimeStepClass,
    List<String> acceptedContracts,
    List<String> acceptedRuntimeClassNames,
    List<Class<?>> acceptedRuntimeTypes,
    boolean terminal
) {

    public boolean accepts(Object item) {
        if (item == null) {
            return false;
        }
        for (Class<?> acceptedRuntimeType : acceptedRuntimeTypes) {
            if (acceptedRuntimeType.isInstance(item)) {
                return true;
            }
        }
        return false;
    }
}
