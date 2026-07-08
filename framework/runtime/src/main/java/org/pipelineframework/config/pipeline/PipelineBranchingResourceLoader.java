package org.pipelineframework.config.pipeline;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jboss.logging.Logger;

/**
 * Loads branch-routing metadata from META-INF resources.
 */
public final class PipelineBranchingResourceLoader {

    private static final Logger LOG = Logger.getLogger(PipelineBranchingResourceLoader.class);
    private static final String RESOURCE = "META-INF/pipeline/branching.json";

    private PipelineBranchingResourceLoader() {
    }

    public static Optional<BranchingResource> load() {
        ClassLoader classLoader = PipelineResources.resolveClassLoader();
        try {
            if (classLoader != null) {
                Enumeration<java.net.URL> resources = classLoader.getResources(RESOURCE);
                List<BranchingResource> candidates = new ArrayList<>();
                while (resources.hasMoreElements()) {
                    java.net.URL url = resources.nextElement();
                    try (InputStream stream = url.openStream()) {
                        candidates.add(parse(stream));
                    }
                }
                if (!candidates.isEmpty()) {
                    return Optional.of(selectBestCandidate(candidates, classLoader));
                }
            }
            InputStream stream = PipelineResources.openResource(classLoader, RESOURCE);
            try (InputStream streamToRead = stream) {
                if (streamToRead == null) {
                    return Optional.empty();
                }
                return Optional.of(parse(streamToRead));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read pipeline branching resource.", e);
        }
    }

    private static BranchingResource parse(InputStream stream) throws Exception {
        Map<?, ?> data = PipelineJson.mapper().readValue(stream, Map.class);
        int terminalStepIndex = intValue(data.get("terminalStepIndex"), -1);
        Object rawSteps = data.get("steps");
        if (!(rawSteps instanceof List<?> list)) {
            return new BranchingResource(terminalStepIndex, List.of());
        }
        List<BranchingStep> steps = list.stream()
            .filter(Map.class::isInstance)
            .map(Map.class::cast)
            .map(PipelineBranchingResourceLoader::parseStep)
            .toList();
        return new BranchingResource(terminalStepIndex, steps);
    }

    private static BranchingStep parseStep(Map<?, ?> raw) {
        String step = stringValue(raw.get("step"));
        String runtimeStepClass = stringValue(raw.get("runtimeStepClass"));
        if (step == null || step.isBlank()) {
            throw new IllegalStateException(
                "Branch-aware step metadata is missing required field 'step' in branching.json. Entry: " + raw);
        }
        if (runtimeStepClass == null || runtimeStepClass.isBlank()) {
            throw new IllegalStateException(
                "Branch-aware step '" + step + "' is missing required field 'runtimeStepClass' in branching.json. Entry: " + raw);
        }
        return new BranchingStep(
            intValue(raw.get("index"), -1),
            step,
            runtimeStepClass,
            stringValue(raw.get("inputRuntimeClass")),
            stringList(raw.get("acceptedContracts")),
            stringList(raw.get("acceptedRuntimeClasses")),
            booleanValue(raw.get("terminal")));
    }

    private static BranchingResource selectBestCandidate(List<BranchingResource> candidates, ClassLoader classLoader) {
        BranchingResource best = candidates.getFirst();
        int bestLoadable = countLoadableSteps(best, classLoader);
        boolean bestAllLoadable = bestLoadable == best.steps().size();
        for (int i = 1; i < candidates.size(); i++) {
            BranchingResource candidate = candidates.get(i);
            int loadable = countLoadableSteps(candidate, classLoader);
            boolean allLoadable = loadable == candidate.steps().size();
            if (allLoadable && !bestAllLoadable) {
                best = candidate;
                bestLoadable = loadable;
                bestAllLoadable = true;
                continue;
            }
            if (allLoadable == bestAllLoadable && loadable > bestLoadable) {
                best = candidate;
                bestLoadable = loadable;
                bestAllLoadable = allLoadable;
            }
        }
        if (!bestAllLoadable) {
            LOG.warnf(
                "Selected pipeline branching metadata has unloadable steps (%d/%d loadable); verify duplicate branching.json resources on classpath.",
                bestLoadable,
                best.steps().size());
            List<BranchingStep> filtered = best.steps().stream()
                .filter(step -> isStepFullyLoadable(step, classLoader))
                .toList();
            return new BranchingResource(best.terminalStepIndex(), filtered);
        }
        return best;
    }

    private static int countLoadableSteps(BranchingResource resource, ClassLoader classLoader) {
        int count = 0;
        for (BranchingStep step : resource.steps()) {
            if (!isLoadable(step.runtimeStepClass(), classLoader)) {
                continue;
            }
            if (step.inputRuntimeClass() != null && !step.inputRuntimeClass().isBlank()
                && !isLoadable(step.inputRuntimeClass(), classLoader)) {
                continue;
            }
            boolean allAcceptedLoadable = true;
            for (String acceptedClass : step.acceptedRuntimeClasses()) {
                if (!isLoadable(acceptedClass, classLoader)) {
                    allAcceptedLoadable = false;
                    break;
                }
            }
            if (allAcceptedLoadable) {
                count++;
            }
        }
        return count;
    }

    private static boolean isLoadable(String className, ClassLoader classLoader) {
        if (className == null || className.isBlank()) {
            return false;
        }
        try {
            Class.forName(className, false, classLoader);
            return true;
        } catch (ClassNotFoundException | LinkageError ignored) {
            return false;
        }
    }

    private static boolean isStepFullyLoadable(BranchingStep step, ClassLoader classLoader) {
        if (!isLoadable(step.runtimeStepClass(), classLoader)) {
            return false;
        }
        if (step.inputRuntimeClass() != null && !step.inputRuntimeClass().isBlank()
            && !isLoadable(step.inputRuntimeClass(), classLoader)) {
            return false;
        }
        for (String acceptedClass : step.acceptedRuntimeClasses()) {
            if (!isLoadable(acceptedClass, classLoader)) {
                return false;
            }
        }
        return true;
    }

    private static int intValue(Object value, int defaultValue) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean booleanValue(Object value) {
        if (value instanceof Boolean flag) {
            return flag;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static List<String> stringList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream()
            .filter(item -> item != null && !String.valueOf(item).isBlank())
            .map(String::valueOf)
            .toList();
    }

    public record BranchingResource(
        int terminalStepIndex,
        List<BranchingStep> steps
    ) {
    }

    public record BranchingStep(
        int index,
        String step,
        String runtimeStepClass,
        String inputRuntimeClass,
        List<String> acceptedContracts,
        List<String> acceptedRuntimeClasses,
        boolean terminal
    ) {
    }
}
