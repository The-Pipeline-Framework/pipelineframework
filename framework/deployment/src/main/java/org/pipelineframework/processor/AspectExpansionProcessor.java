package org.pipelineframework.processor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.pipelineframework.processor.ir.*;

/**
 * Processor that expands pipeline aspects into synthetic steps during pipeline compilation.
 * This processor handles the semantic expansion of aspects into identity side effect steps
 * without changing the pipeline's functional shape.
 */
public class AspectExpansionProcessor {

    /**
     * Default constructor for AspectExpansionProcessor.
     */
    public AspectExpansionProcessor() {
    }
    
    /**
     * Expands pipeline aspects into synthetic steps during pipeline compilation.
     * This method handles the semantic expansion of aspects into identity side effect steps
     * without changing the pipeline's functional shape.
     *
     * @param originalSteps The original pipeline steps before aspect expansion
     * @param aspects The aspects to be expanded into synthetic steps
     * @return A list of pipeline steps with aspects expanded as synthetic steps
     */
    public List<PipelineStepModel> expandAspects(
            List<PipelineStepModel> originalSteps,
            List<PipelineAspectModel> aspects) {
        
        // Create a snapshot of original steps to prevent reprocessing synthetic steps
        List<PipelineStepModel> originalStepsSnapshot = new ArrayList<>(originalSteps);
        
        // Separate GLOBAL and STEP-scoped aspects
        List<PipelineAspectModel> globalAspects = aspects.stream()
                .filter(aspect -> aspect.scope() == AspectScope.GLOBAL)
                .sorted(Comparator.comparingInt(PipelineAspectModel::order))
                .toList();

        List<PipelineAspectModel> stepAspects = aspects.stream()
                .filter(aspect -> aspect.scope() == AspectScope.STEPS)
                .sorted(Comparator.comparingInt(PipelineAspectModel::order))
                .collect(Collectors.toList());
        
        // Validate step targeting for STEP-scoped aspects
        validateStepTargeting(originalStepsSnapshot, stepAspects);
        
        List<PipelineStepModel> expandedSteps = new ArrayList<>();
        
        for (PipelineStepModel originalStep : originalStepsSnapshot) {
            // Apply GLOBAL aspects BEFORE the step first
            for (PipelineAspectModel aspect : globalAspects) {
                if (aspect.position() == AspectPosition.BEFORE_STEP) {
                    expandedSteps.add(createSyntheticStep(originalStep, aspect));
                }
            }

            // Apply STEP aspects BEFORE the step
            for (PipelineAspectModel aspect : stepAspects) {
                @SuppressWarnings("unchecked")
                Set<String> targetSteps = (Set<String>) aspect.config().get("targetSteps");
                if (aspect.position() == AspectPosition.BEFORE_STEP &&
                    targetSteps != null && targetSteps.contains(originalStep.serviceName())) {
                    expandedSteps.add(createSyntheticStep(originalStep, aspect));
                }
            }
            
            // Add the original step
            expandedSteps.add(originalStep);
            
            // Apply STEP aspects AFTER the step first (in reverse order to maintain precedence)
            for (int i = stepAspects.size() - 1; i >= 0; i--) {
                PipelineAspectModel aspect = stepAspects.get(i);
                @SuppressWarnings("unchecked")
                Set<String> targetSteps = (Set<String>) aspect.config().get("targetSteps");
                if (aspect.position() == AspectPosition.AFTER_STEP &&
                    targetSteps != null && targetSteps.contains(originalStep.serviceName())) {
                    expandedSteps.add(createSyntheticStep(originalStep, aspect));
                }
            }

            // Apply GLOBAL aspects AFTER the step
            for (PipelineAspectModel aspect : globalAspects) {
                if (aspect.position() == AspectPosition.AFTER_STEP) {
                    expandedSteps.add(createSyntheticStep(originalStep, aspect));
                }
            }
        }
        
        return expandedSteps;
    }
    
    private PipelineStepModel createSyntheticStep(
            PipelineStepModel originalStep,
            PipelineAspectModel aspect) {
        
        String syntheticName = "Process" +
                capitalize(originalStep.serviceClassName().simpleName()) +
                capitalize(aspect.name()) +
                (aspect.position() == AspectPosition.BEFORE_STEP ? "Before" : "After");
        
        // Resolve the plugin service package from the aspect's plugin implementation
        String pluginServiceClass = (String) aspect.config().get("pluginImplementationClass");
        if (pluginServiceClass == null) {
            throw new IllegalArgumentException("Aspect '" + aspect.name() +
                "' must specify pluginImplementationClass in config");
        }
        String pluginPackage = extractPackage(pluginServiceClass);
        String pluginSimpleClassName = pluginServiceClass.substring(pluginServiceClass.lastIndexOf('.') + 1);

        // Recompute generation targets based on plugin service configuration
        Set<GenerationTarget> recomputedTargets = computeGenerationTargets(aspect);

        return new PipelineStepModel.Builder()
                .servicePackage(pluginPackage)
                .serviceClassName(com.squareup.javapoet.ClassName.get(pluginPackage, pluginSimpleClassName))
                .serviceName(syntheticName)
                .inputMapping(originalStep.inputMapping())
                .outputMapping(originalStep.outputMapping())
                .streamingShape(originalStep.streamingShape())
                .executionMode(originalStep.executionMode())
                .enabledTargets(recomputedTargets) // Recompute targets
                .build();
    }
    
    private void validateStepTargeting(
            List<PipelineStepModel> originalSteps,
            List<PipelineAspectModel> stepAspects) {
        
        Set<String> availableStepNames = originalSteps.stream()
                .map(PipelineStepModel::serviceName)
                .collect(Collectors.toSet());

        for (PipelineAspectModel aspect : stepAspects) {
            @SuppressWarnings("unchecked")
            Set<String> targetSteps = (Set<String>) aspect.config().get("targetSteps");
            if (targetSteps != null) {
                for (String targetStep : targetSteps) {
                    if (!availableStepNames.contains(targetStep)) {
                        throw new IllegalArgumentException(
                            "STEP-scoped aspect '" + aspect.name() +
                            "' targets non-existent step: " + targetStep);
                    }
                }
            }
        }
    }
    
    private String extractPackage(String fullyQualifiedClassName) {
        int lastDotIndex = fullyQualifiedClassName.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return fullyQualifiedClassName.substring(0, lastDotIndex);
        }
        return ""; // Default package
    }
    
    private Set<GenerationTarget> computeGenerationTargets(PipelineAspectModel aspect) {
        // Compute targets based on plugin service configuration
        // This would typically come from the plugin service metadata in the config
        @SuppressWarnings("unchecked")
        Set<GenerationTarget> targets = (Set<GenerationTarget>) aspect.config().get("enabledTargets");
        return targets != null ? targets : Set.of(); // Return empty set if not specified
    }

    private String capitalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return input.substring(0, 1).toUpperCase() + input.substring(1);
    }
}