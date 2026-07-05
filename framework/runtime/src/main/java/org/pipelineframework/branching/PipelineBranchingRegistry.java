package org.pipelineframework.branching;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import org.pipelineframework.config.pipeline.PipelineBranchingResourceLoader;

/**
 * Holds runtime branch-routing descriptors keyed by runtime step class.
 */
@Startup
@ApplicationScoped
@Unremovable
public class PipelineBranchingRegistry {

    private final Map<String, StepBranchingDescriptor> descriptorsByStepClass;

    public PipelineBranchingRegistry() {
        this(PipelineBranchingResourceLoader.load().orElse(null));
    }

    PipelineBranchingRegistry(PipelineBranchingResourceLoader.BranchingResource resource) {
        this.descriptorsByStepClass = resource == null ? Map.of() : buildDescriptors(resource);
    }

    public Optional<StepBranchingDescriptor> descriptorFor(Class<?> stepClass) {
        if (stepClass == null || descriptorsByStepClass.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(descriptorsByStepClass.get(normalizeStepClassName(stepClass)));
    }

    private Map<String, StepBranchingDescriptor> buildDescriptors(PipelineBranchingResourceLoader.BranchingResource resource) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Map<String, StepBranchingDescriptor> descriptors = new LinkedHashMap<>();
        for (PipelineBranchingResourceLoader.BranchingStep step : resource.steps()) {
            String runtimeStepClass = step.runtimeStepClass();
            if (runtimeStepClass == null || runtimeStepClass.isBlank()) {
                throw new IllegalStateException(
                    "Branch-aware step '" + step.step() + "' at index " + step.index()
                        + " has null or blank runtimeStepClass in branching metadata. The branching.json resource is malformed.");
            }
            // Verify runtimeStepClass is loadable; defer resolution of input and accepted types
            try {
                Class.forName(runtimeStepClass, false, classLoader);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                    "Failed to resolve runtime step class '" + runtimeStepClass + "' for branch-aware step '" + step.step() + "'", e);
            }
            List<Class<?>> acceptedRuntimeTypes = new java.util.ArrayList<>();
            for (String className : step.acceptedRuntimeClasses()) {
                acceptedRuntimeTypes.add(resolveSoftClass(className, classLoader));
            }
            Class<?> inputRuntimeType = resolveSoftClass(step.inputRuntimeClass(), classLoader);
            StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
                step.index(),
                step.step(),
                runtimeStepClass,
                step.inputRuntimeClass(),
                inputRuntimeType,
                step.acceptedContracts(),
                step.acceptedRuntimeClasses(),
                acceptedRuntimeTypes,
                step.terminal());
            StepBranchingDescriptor existing = descriptors.put(runtimeStepClass, descriptor);
            if (existing != null) {
                throw new IllegalStateException(
                    "Duplicate runtimeStepClass '" + runtimeStepClass
                    + "' detected: step '" + step.step() + "' at index " + step.index()
                    + " conflicts with existing step '" + existing.stepName() + "' at index " + existing.index());
            }
        }
        return Map.copyOf(descriptors);
    }

    private Class<?> resolveClass(String className, ClassLoader classLoader, String stepName) {
        try {
            return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed to resolve accepted runtime class '" + className + "' for branch-aware step '" + stepName + "'", e);
        }
    }

    private Class<?> resolveOptionalClass(String className, ClassLoader classLoader, String stepName, String label) {
        if (className == null || className.isBlank()) {
            return null;
        }
        try {
            return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed to resolve " + label + " class '" + className + "' for branch-aware step '" + stepName + "'",
                e);
        }
    }

    private Class<?> resolveSoftClass(String className, ClassLoader classLoader) {
        if (className == null || className.isBlank()) {
            return null;
        }
        try {
            return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private String normalizeStepClassName(Class<?> stepClass) {
        String name = stepClass.getName();
        if ((name.contains("_Subclass") || name.contains("$$") || name.contains("_ClientProxy"))
            && stepClass.getSuperclass() != null) {
            return stepClass.getSuperclass().getName();
        }
        return name;
    }
}
