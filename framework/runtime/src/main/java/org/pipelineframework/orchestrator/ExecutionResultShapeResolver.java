package org.pipelineframework.orchestrator;

import java.nio.file.Path;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlStep;

/**
 * Resolves the persisted queue-async terminal result shape from pipeline YAML.
 */
@ApplicationScoped
public class ExecutionResultShapeResolver {

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
        Path configPath = resolveConfigPath();
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        if (config.steps().isEmpty()) {
            throw new IllegalStateException("Queue-async result-shape resolution requires at least one pipeline step");
        }
        ExecutionResultShape shape = ExecutionResultShape.SINGLE;
        for (PipelineYamlStep step : config.steps()) {
            CardinalitySemantics cardinality = CardinalitySemantics.fromString(step.cardinality());
            shape = switch (cardinality) {
                case ONE_TO_ONE -> shape;
                case ONE_TO_MANY, MANY_TO_MANY -> ExecutionResultShape.MATERIALIZED_MULTI;
                case MANY_TO_ONE -> ExecutionResultShape.SINGLE;
            };
        }
        return shape;
    }

    private static Path resolveConfigPath() {
        String explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit != null) {
            return Path.of(explicit).toAbsolutePath().normalize();
        }
        return new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath())
            .orElseThrow(() -> new IllegalStateException("No pipeline YAML found for queue-async result-shape resolution"));
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
