package org.pipelineframework.orchestrator;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

/**
 * Resolves the persisted queue-async terminal result shape from pipeline YAML.
 */
@ApplicationScoped
public class ExecutionResultShapeResolver {

    @Inject
    PipelineContractDescriptorLoader contractLoader;

    private volatile ExecutionResultShape cachedShape;

    /**
     * Returns the terminal result shape for the configured pipeline.
     */
    public ExecutionResultShape resolve() {
        ExecutionResultShape shape = cachedShape;
        if (shape != null) {
            return shape;
        }
        synchronized (this) {
            if (cachedShape == null) {
                cachedShape = loadShape();
            }
            return cachedShape;
        }
    }

    private ExecutionResultShape loadShape() {
        Optional<Path> explicitConfigPath = explicitConfigPath();
        if (explicitConfigPath.isPresent()) {
            return loadYamlShape(explicitConfigPath.orElseThrow());
        }
        Optional<PipelineContractDescriptor> generatedContract = contractLoader().load()
            .filter(contract -> !contract.steps().isEmpty());
        if (generatedContract.isPresent()) {
            return resolveShape(generatedContract.orElseThrow().steps().stream()
                .map(PipelineBundleStepDescriptor::cardinality)
                .toList());
        }
        Path configPath = new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath())
            .orElseThrow(() -> new IllegalStateException(
                "No generated pipeline contract or pipeline YAML found for queue-async result-shape resolution"));
        return loadYamlShape(configPath);
    }

    private static ExecutionResultShape loadYamlShape(Path configPath) {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        if (config.steps().isEmpty()) {
            throw new IllegalStateException("Queue-async result-shape resolution requires at least one pipeline step");
        }
        return resolveShape(config.steps().stream().map(PipelineYamlStep::cardinality).toList());
    }

    private static ExecutionResultShape resolveShape(List<String> cardinalities) {
        ExecutionResultShape shape = ExecutionResultShape.SINGLE;
        for (String configuredCardinality : cardinalities) {
            CardinalitySemantics cardinality = CardinalitySemantics.fromString(configuredCardinality);
            shape = switch (cardinality) {
                case ONE_TO_ONE -> shape;
                case ONE_TO_MANY, MANY_TO_MANY -> ExecutionResultShape.MATERIALIZED_MULTI;
                case MANY_TO_ONE -> ExecutionResultShape.SINGLE;
            };
        }
        return shape;
    }

    private static Optional<Path> explicitConfigPath() {
        String explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        return explicit == null
            ? Optional.empty()
            : Optional.of(Path.of(explicit).toAbsolutePath().normalize());
    }

    private PipelineContractDescriptorLoader contractLoader() {
        return contractLoader == null ? new PipelineContractDescriptorLoader() : contractLoader;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
