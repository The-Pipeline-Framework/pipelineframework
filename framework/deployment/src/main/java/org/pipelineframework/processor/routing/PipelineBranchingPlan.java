package org.pipelineframework.processor.routing;

import java.util.List;
import com.squareup.javapoet.ClassName;

/**
 * Build-time branch-routing plan derived from the authored linear pipeline sequence.
 */
public record PipelineBranchingPlan(
    boolean branchAware,
    int terminalStepIndex,
    List<BranchStep> steps
) {

    public static PipelineBranchingPlan disabled() {
        return new PipelineBranchingPlan(false, -1, List.of());
    }

    public record BranchStep(
        int index,
        String stepName,
        String inputContractName,
        String outputContractName,
        List<String> acceptedContractTypes,
        List<String> producedLeafContractTypes,
        List<ClassName> acceptedDomainTypes,
        boolean terminal
    ) {
    }
}
