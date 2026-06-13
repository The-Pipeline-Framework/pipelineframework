package org.pipelineframework.config.composition;

import java.nio.file.Path;
import java.util.Optional;

import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;

/**
 * Resolved pipeline node in a composition IR.
 *
 * @param id pipeline id from the composition manifest
 * @param path resolved pipeline YAML path
 * @param config parsed pipeline template config
 */
public record PipelineCompositionNode(
    String id,
    Path path,
    PipelineTemplateConfig config
) {
    public PipelineCompositionNode {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("composition node id must not be blank");
        }
        if (path == null) {
            throw new IllegalArgumentException("composition node path must not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("composition node config must not be null");
        }
        id = id.trim();
        path = path.toAbsolutePath().normalize();
    }

    public PipelineTemplateStep entryStep() {
        return config.steps().getFirst();
    }

    public PipelineTemplateStep terminalStep() {
        return config.steps().getLast();
    }

    public Optional<String> subscriptionPublication() {
        if (config.input() == null || config.input().subscription() == null) {
            return Optional.empty();
        }
        return Optional.of(config.input().subscription().publication());
    }

    public Optional<String> checkpointPublication() {
        if (config.output() == null || config.output().checkpoint() == null) {
            return Optional.empty();
        }
        return Optional.of(config.output().checkpoint().publication());
    }
}
