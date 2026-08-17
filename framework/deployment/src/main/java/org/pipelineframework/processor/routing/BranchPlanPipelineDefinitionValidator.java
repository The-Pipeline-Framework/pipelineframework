/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor.routing;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.pipelineframework.processor.composition.PipelineDefinition;
import org.pipelineframework.processor.composition.PipelineDefinitionStep;
import org.pipelineframework.processor.composition.PipelineDefinitionValidator;
import org.pipelineframework.processor.composition.PipelineReference;

/**
 * Adapts an authoritative {@link PipelineBranchingPlan} to source-neutral composition validation.
 *
 * <p>The planner remains the sole authority for union expansion and reachability. This adapter
 * only verifies that the composition projection preserves the planner's already-resolved routing
 * declarations.
 */
public final class BranchPlanPipelineDefinitionValidator implements PipelineDefinitionValidator {

    private final Map<PipelineReference, PipelineBranchingPlan> plans;

    public BranchPlanPipelineDefinitionValidator(Map<PipelineReference, PipelineBranchingPlan> plans) {
        this.plans = Map.copyOf(Objects.requireNonNull(plans, "plans must not be null"));
    }

    @Override
    public void validate(PipelineDefinition definition) {
        PipelineBranchingPlan plan = plans.get(definition.reference());
        if (plan == null || !plan.branchAware()) {
            return;
        }
        if (plan.steps().size() != definition.steps().size()) {
            throw new IllegalArgumentException("Branch plan step count does not match definition: "
                + definition.reference().logicalId());
        }
        for (int index = 0; index < definition.steps().size(); index++) {
            PipelineDefinitionStep step = definition.steps().get(index);
            PipelineBranchingPlan.BranchStep planned = plan.steps().get(index);
            if (!step.localStepId().equals(planned.stepName())
                || !step.inputContractId().equals(planned.inputContractName())
                || !step.outputContractId().equals(planned.outputContractName())
                || !step.acceptedContractIds().equals(planned.acceptedContractTypes())
                || step.terminal() != planned.terminal()) {
                throw new IllegalArgumentException("Composition definition does not preserve branch plan semantics at step '"
                    + step.localStepId() + "'.");
            }
        }
    }
}
