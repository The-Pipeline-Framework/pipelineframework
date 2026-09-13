/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.processor.phase;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.routing.PipelineBranchRoutingPlanner;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;

/**
 * Builds the compiler-owned branch plan before Java step models are extracted.
 *
 * <p>The early position lets framework-owned generated steps derive their effective Java boundary
 * from the same routing decision that later drives execution metadata.</p>
 */
public final class PipelineBranchPlanningPhase implements PipelineCompilationPhase {

    @Override
    public String name() {
        return "Pipeline Branch Planning Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) {
        ctx.setBranchingPlan(new PipelineBranchRoutingPlanner().plan(ctx)
            .orElseGet(PipelineBranchingPlan::disabled));
    }
}
