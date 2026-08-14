package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDefinition;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.composition.PipelineDefinition;
import org.pipelineframework.processor.composition.PipelineDefinitionLinker;
import org.pipelineframework.processor.composition.PipelineDefinitionStep;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.composition.ResolvedPipelineDefinitionGraph;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.routing.BranchPlanPipelineDefinitionValidator;
import org.pipelineframework.processor.routing.PipelineBranchRoutingPlanner;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;
import org.pipelineframework.processor.routing.V3PipelineInvocationCompatibility;

/** Links the v3 local definition catalog using the existing branch planner as routing authority. */
final class PipelineCompositionLinkingPhase {
    private static final PipelineReference ROOT = new PipelineReference("$root");

    void link(PipelineCompilationContext ctx) {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || (config.pipelines().isEmpty() && !containsPipelineReference(config.steps()))) {
            return;
        }
        if (!ctx.isTransportModeLocal()) {
            throw new IllegalArgumentException("Local pipeline invocation currently requires pipeline.transport=LOCAL; "
                + "remote pipeline invocation is not implemented in this slice.");
        }
        Map<String, List<StepDefinition>> parsed = ctx.getParsedPipelineDefinitionCatalog().localDefinitions();
        Map<PipelineReference, PipelineBranchingPlan> branchPlans = new LinkedHashMap<>();
        branchPlans.put(ROOT, ctx.getBranchingPlan() == null ? PipelineBranchingPlan.disabled() : ctx.getBranchingPlan());
        Map<PipelineReference, PipelineDefinition> definitions = new LinkedHashMap<>();
        definitions.put(ROOT, definition(ROOT, config.inputContract(), config.outputContract(), config.steps(),
            ctx.getStepDefinitions(), branchPlans.get(ROOT)));
        for (var entry : config.pipelines().entrySet()) {
            PipelineReference reference = new PipelineReference(entry.getKey());
            PipelineTemplateDefinition fragment = entry.getValue();
            List<StepDefinition> parsedSteps = parsed.get(reference.logicalId());
            if (parsedSteps == null) {
                throw new IllegalArgumentException("Pipeline definition '" + reference.logicalId()
                    + "' was not parsed through the v3 step grammar.");
            }
            PipelineTemplateConfig scoped = scoped(config, fragment);
            Object previousConfig = ctx.getPipelineTemplateConfig();
            List<StepDefinition> previousSteps = ctx.getStepDefinitions();
            try {
                ctx.setPipelineTemplateConfig(scoped);
                ctx.setStepDefinitions(parsedSteps);
                branchPlans.put(reference, new PipelineBranchRoutingPlanner().plan(ctx)
                    .orElseThrow(() -> new IllegalArgumentException("Invalid v3 routing in pipeline definition '"
                        + reference.logicalId() + "'.")));
            } finally {
                ctx.setPipelineTemplateConfig(previousConfig);
                ctx.setStepDefinitions(previousSteps);
            }
            definitions.put(reference, definition(reference, fragment.inputContract(), fragment.outputContract(),
                fragment.steps(), parsedSteps, branchPlans.get(reference)));
        }
        ResolvedPipelineDefinitionGraph graph = new PipelineDefinitionLinker(
            reference -> Optional.ofNullable(definitions.get(reference)),
            new V3PipelineInvocationCompatibility(config.typeModel()),
            new BranchPlanPipelineDefinitionValidator(branchPlans)).link(definitions.get(ROOT));
        ctx.setResolvedPipelineDefinitionGraph(graph);
        Map<PipelineReference, PipelineBranchingPlan> childPlans = new LinkedHashMap<>(branchPlans);
        childPlans.remove(ROOT);
        ctx.setLocalDefinitionBranchingPlans(Map.copyOf(childPlans));
    }

    private boolean containsPipelineReference(List<PipelineTemplateStep> steps) {
        return steps.stream().anyMatch(step -> step.pipelineReference() != null
            && !step.pipelineReference().isBlank());
    }

    private PipelineDefinition definition(PipelineReference reference, String input, String output,
            List<PipelineTemplateStep> steps, List<StepDefinition> parsed, PipelineBranchingPlan branchPlan) {
        Map<String, StepDefinition> parsedByName = new LinkedHashMap<>();
        for (StepDefinition step : parsed) {
            parsedByName.put(step.name(), step);
        }
        List<PipelineDefinitionStep> projected = new ArrayList<>();
        for (int index = 0; index < steps.size(); index++) {
            PipelineTemplateStep step = steps.get(index);
            StepDefinition parsedStep = parsedByName.get(step.name());
            if (parsedStep == null) {
                throw new IllegalArgumentException("Step '" + step.name() + "' in definition '"
                    + reference.logicalId() + "' was not parsed.");
            }
            List<String> resolvedAccepts = branchPlan != null && branchPlan.branchAware()
                ? branchPlan.steps().get(index).acceptedContractTypes()
                : step.accepts();
            if (parsedStep.pipelineReference().isPresent()) {
                projected.add(PipelineDefinitionStep.pipeline(step.name(), step.inputTypeName(), step.outputTypeName(),
                    new PipelineReference(parsedStep.pipelineReference().orElseThrow()), resolvedAccepts, step.terminal()));
            } else {
                projected.add(PipelineDefinitionStep.direct(step.name(), step.inputTypeName(), step.outputTypeName(),
                    cardinality(step, parsedStep), resolvedAccepts, step.terminal()));
            }
        }
        return new PipelineDefinition(reference, input, output, projected);
    }

    private CardinalitySemantics cardinality(PipelineTemplateStep step, StepDefinition parsed) {
        if (step.cardinality() != null && !step.cardinality().isBlank()) {
            return CardinalitySemantics.fromString(step.cardinality());
        }
        StreamingShape shape = parsed.streamingShapeHint();
        if (shape == null) {
            throw new IllegalArgumentException("Step '" + step.name() + "' must declare cardinality for composition.");
        }
        return switch (shape) {
            case UNARY_UNARY -> CardinalitySemantics.ONE_TO_ONE;
            case UNARY_STREAMING -> CardinalitySemantics.ONE_TO_MANY;
            case STREAMING_UNARY -> CardinalitySemantics.MANY_TO_ONE;
            case STREAMING_STREAMING -> CardinalitySemantics.MANY_TO_MANY;
        };
    }

    private PipelineTemplateConfig scoped(PipelineTemplateConfig root, PipelineTemplateDefinition definition) {
        return new PipelineTemplateConfig(root.version(), root.appName(), root.basePackage(), root.transport(), root.platform(),
            root.messages(), root.unions(), root.sources(), root.publish(), definition.steps(), root.aspects(), root.input(),
            root.output(), root.materialization(), definition.inputContract(), definition.outputContract(), root.typeModel());
    }
}
