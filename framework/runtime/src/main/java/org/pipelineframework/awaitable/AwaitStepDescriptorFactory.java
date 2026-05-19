package org.pipelineframework.awaitable;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;

/**
 * Builds await descriptors from runtime pipeline YAML.
 */
@ApplicationScoped
public class AwaitStepDescriptorFactory {
    private final Map<String, AwaitStepDescriptor> descriptors = new ConcurrentHashMap<>();

    /**
     * Resolves the descriptor for a generated await step.
     */
    public AwaitStepDescriptor descriptor(String serviceName, String inputType, String outputType) {
        return descriptors.computeIfAbsent(serviceName, key -> loadDescriptor(key, inputType, outputType));
    }

    private AwaitStepDescriptor loadDescriptor(String serviceName, String inputType, String outputType) {
        Path base = Path.of("").toAbsolutePath();
        Path configPath = new PipelineYamlConfigLocator().locate(base)
            .orElseThrow(() -> new IllegalStateException("No pipeline YAML found for await step " + serviceName));
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        PipelineYamlStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No await YAML step found for generated service " + serviceName));
        if (!"await".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated await service " + serviceName + " maps to non-await YAML step");
        }
        if (step.awaitConfig() == null || step.awaitConfig().transport() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await.transport configuration");
        }
        return new AwaitStepDescriptor(
            serviceName,
            inputType,
            outputType,
            Duration.parse(step.timeout()),
            step.awaitConfig().correlation().strategy(),
            step.awaitConfig().transport().type(),
            step.awaitConfig().transport().config(),
            step.idempotencyKeyFields());
    }

    private static String toServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String stripped = stepName.startsWith("Process ") ? stepName.substring("Process ".length()) : stepName;
        StringBuilder formatted = new StringBuilder();
        for (String part : stripped.split(" ")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            formatted.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                formatted.append(lower.substring(1));
            }
        }
        return formatted.isEmpty() ? "ProcessStepService" : "Process" + formatted + "Service";
    }
}
