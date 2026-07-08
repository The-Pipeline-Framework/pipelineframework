package org.pipelineframework.processor.routing;

import java.util.List;
import java.util.Objects;
import com.squareup.javapoet.ClassName;

/**
 * Build-time branch-routing plan derived from the authored linear pipeline sequence.
 */
public record PipelineBranchingPlan(
    boolean branchAware,
    int terminalStepIndex,
    List<BranchStep> steps
) {

    public PipelineBranchingPlan {
        steps = steps == null ? List.of() : List.copyOf(steps);
        if (!branchAware) {
            if (terminalStepIndex != -1 || !steps.isEmpty()) {
                throw new IllegalArgumentException("Disabled branching plans must use terminalStepIndex=-1 and no steps.");
            }
        } else {
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("Branch-aware plans must contain at least one step.");
            }
            for (int i = 0; i < steps.size(); i++) {
                if (steps.get(i).index() != i) {
                    throw new IllegalArgumentException(
                        "BranchStep index mismatch at position " + i + ": expected " + i + " but was "
                            + steps.get(i).index() + ".");
                }
            }
            if (terminalStepIndex < 0 || terminalStepIndex >= steps.size()) {
                throw new IllegalArgumentException("terminalStepIndex out of bounds for branch-aware plan.");
            }
            if (!steps.get(terminalStepIndex).terminal()) {
                throw new IllegalArgumentException("terminalStepIndex must point at a terminal branch step.");
            }
        }
    }

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
        public BranchStep {
            stepName = Objects.requireNonNull(stepName, "stepName");
            inputContractName = Objects.requireNonNull(inputContractName, "inputContractName");
            outputContractName = Objects.requireNonNull(outputContractName, "outputContractName");
            acceptedContractTypes = acceptedContractTypes == null ? List.of() : List.copyOf(acceptedContractTypes);
            producedLeafContractTypes = producedLeafContractTypes == null ? List.of() : List.copyOf(producedLeafContractTypes);
            acceptedDomainTypes = acceptedDomainTypes == null ? List.of() : List.copyOf(acceptedDomainTypes);
            if (stepName.isBlank() || inputContractName.isBlank() || outputContractName.isBlank()) {
                throw new IllegalArgumentException("BranchStep names and contract names must be non-blank.");
            }
            if (acceptedContractTypes.isEmpty()) {
                throw new IllegalArgumentException("BranchStep must declare at least one accepted contract type.");
            }
            if (producedLeafContractTypes.isEmpty()) {
                throw new IllegalArgumentException("BranchStep must declare at least one produced leaf contract type.");
            }
            if (acceptedDomainTypes.isEmpty()) {
                throw new IllegalArgumentException("BranchStep must declare accepted domain types.");
            }
        }
    }
}
