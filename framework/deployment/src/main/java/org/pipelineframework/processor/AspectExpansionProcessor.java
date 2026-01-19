package org.pipelineframework.processor;

import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
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
    public List<ResolvedStep> expandAspects(
            List<ResolvedStep> originalSteps,
            List<PipelineAspectModel> aspects) {
        
        // Create a snapshot of original steps to prevent reprocessing synthetic steps
        List<ResolvedStep> originalStepsSnapshot = new ArrayList<>(originalSteps);
        
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
        
        List<ResolvedStep> expandedSteps = new ArrayList<>();
        
        Set<String> generatedKeys = new java.util.HashSet<>();

        for (ResolvedStep originalStep : originalStepsSnapshot) {
            PipelineStepModel originalModel = originalStep.model();

            List<PipelineAspectModel> applicable = new ArrayList<>();
            applicable.addAll(globalAspects);
            for (PipelineAspectModel aspect : stepAspects) {
                Set<String> targetSteps = extractTargetSteps(aspect);
                if (targetSteps != null && targetSteps.contains(originalModel.serviceName())) {
                    applicable.add(aspect);
                }
            }

            List<PipelineAspectModel> beforeAspects = applicable.stream()
                .filter(aspect -> aspect.position() == AspectPosition.BEFORE_STEP)
                .sorted(Comparator.comparingInt(PipelineAspectModel::order))
                .collect(Collectors.toList());
            List<PipelineAspectModel> afterAspects = applicable.stream()
                .filter(aspect -> aspect.position() == AspectPosition.AFTER_STEP)
                .sorted(Comparator.comparingInt(PipelineAspectModel::order))
                .collect(Collectors.toList());

            for (PipelineAspectModel aspect : beforeAspects) {
                ResolvedStep synthetic = createSyntheticStep(originalStep, aspect, AspectPosition.BEFORE_STEP);
                String key = synthetic.model().serviceName() + ":" + aspect.name() + ":before";
                if (generatedKeys.add(key)) {
                    expandedSteps.add(synthetic);
                }
            }

            expandedSteps.add(originalStep);

            for (PipelineAspectModel aspect : afterAspects) {
                ResolvedStep synthetic = createSyntheticStep(originalStep, aspect, AspectPosition.AFTER_STEP);
                String key = synthetic.model().serviceName() + ":" + aspect.name() + ":after";
                if (generatedKeys.add(key)) {
                    expandedSteps.add(synthetic);
                }
            }
        }
        
        return expandedSteps;
    }
    
    private ResolvedStep createSyntheticStep(
            ResolvedStep originalStep,
            PipelineAspectModel aspect,
            AspectPosition position) {

        PipelineStepModel originalModel = originalStep.model();
        String messageName = resolveMessageName(originalStep, position);
        String serviceName = "Observe" + toPascalCase(aspect.name()) + messageName + "SideEffectService";
        String syntheticName = toPascalCase(aspect.name()) + messageName + "SideEffect";
        
        // Resolve the plugin service package from the aspect's plugin implementation
        String pluginServiceClass = (String) aspect.config().get("pluginImplementationClass");
        if (pluginServiceClass == null) {
            throw new IllegalArgumentException("Aspect '" + aspect.name() +
                "' must specify pluginImplementationClass in config");
        }
        String pluginPackage = extractPackage(pluginServiceClass);
        String pluginSimpleClassName = pluginServiceClass.substring(pluginServiceClass.lastIndexOf('.') + 1);

        Set<GenerationTarget> recomputedTargets = Set.copyOf(originalModel.enabledTargets());

        TypeMapping mapping = position == AspectPosition.BEFORE_STEP
            ? originalModel.inputMapping()
            : originalModel.outputMapping();
        if (mapping.domainType() == null) {
            throw new IllegalArgumentException(
                "Aspect '" + aspect.name() + "' requires a " +
                    (position == AspectPosition.BEFORE_STEP ? "input" : "output") +
                    " mapping for step '" +
                    originalModel.serviceName() + "'");
        }

        PipelineStepModel syntheticModel = new PipelineStepModel.Builder()
                .serviceName(serviceName)
                .generatedName(syntheticName)
                .servicePackage(originalModel.servicePackage())
                .serviceClassName(com.squareup.javapoet.ClassName.get(pluginPackage, pluginSimpleClassName))
                .inputMapping(mapping)
                .outputMapping(mapping)
                .streamingShape(StreamingShape.UNARY_UNARY)
                .executionMode(originalModel.executionMode())
                .enabledTargets(recomputedTargets) // Recompute targets
                .deploymentRole(DeploymentRole.PLUGIN_SERVER)
                .sideEffect(true)
                .cacheKeyGenerator(originalModel.cacheKeyGenerator())
                .build();

        return new ResolvedStep(syntheticModel, null, null);
    }
    
    private void validateStepTargeting(
            List<ResolvedStep> originalSteps,
            List<PipelineAspectModel> stepAspects) {
        Set<String> availableStepNames = originalSteps.stream()
                .map(step -> step.model().serviceName())
                .collect(Collectors.toSet());

        if (availableStepNames.size() <= 1) {
            return;
        }

        for (PipelineAspectModel aspect : stepAspects) {
            Set<String> targetSteps = extractTargetSteps(aspect);
            if (targetSteps == null) {
                continue;
            }
            for (String targetStep : targetSteps) {
                validateTargetStepName(availableStepNames, aspect.name(), targetStep);
            }
        }
    }

    private void validateTargetStepName(Set<String> availableStepNames, String aspectName, String targetStep) {
        if (!availableStepNames.contains(targetStep)) {
            throw new IllegalArgumentException(
                "STEP-scoped aspect '" + aspectName +
                    "' targets non-existent step: " + targetStep);
        }
    }

    private Set<String> extractTargetSteps(PipelineAspectModel aspect) {
        Object targetStepsValue = aspect.config().get("targetSteps");
        if (targetStepsValue == null) {
            return null;
        }
        Set<String> targetSteps = new java.util.LinkedHashSet<>();
        if (targetStepsValue instanceof Iterable<?> iterable) {
            for (Object entry : iterable) {
                if (entry != null) {
                    targetSteps.add(entry.toString());
                }
            }
        } else {
            targetSteps.add(targetStepsValue.toString());
        }
        return targetSteps.isEmpty() ? null : targetSteps;
    }
    
    private String extractPackage(String fullyQualifiedClassName) {
        int lastDotIndex = fullyQualifiedClassName.lastIndexOf('.');
        if (lastDotIndex > 0) {
            return fullyQualifiedClassName.substring(0, lastDotIndex);
        }
        return ""; // Default package
    }
    
    private String resolveMessageName(ResolvedStep originalStep, AspectPosition position) {
        if (originalStep.grpcBinding() != null
            && originalStep.grpcBinding().methodDescriptor() instanceof Descriptors.MethodDescriptor methodDescriptor) {
            return position == AspectPosition.BEFORE_STEP
                ? methodDescriptor.getInputType().getName()
                : methodDescriptor.getOutputType().getName();
        }

        PipelineStepModel model = originalStep.model();
        TypeMapping mapping = position == AspectPosition.BEFORE_STEP
            ? model.inputMapping()
            : model.outputMapping();
        if (mapping.domainType() == null) {
            throw new IllegalStateException(
                "Missing domain type for step '" + model.serviceName() +
                    "'; cannot derive side-effect service name.");
        }
        return simpleTypeName(mapping.domainType());
    }

    private String simpleTypeName(TypeName type) {
        if (type instanceof ClassName className) {
            return className.simpleName();
        }
        String name = type.toString();
        int generics = name.indexOf('<');
        if (generics != -1) {
            name = name.substring(0, generics);
        }
        int lastDot = name.lastIndexOf('.');
        String simple = lastDot == -1 ? name : name.substring(lastDot + 1);
        return simple.replace("[]", "");
    }

    private String toPascalCase(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }
        StringBuilder builder = new StringBuilder();
        String[] parts = input.split("[^a-zA-Z0-9]+");
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase(Locale.ROOT));
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.toString();
    }
}
