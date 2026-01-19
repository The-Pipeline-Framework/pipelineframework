package org.pipelineframework.processor.phase;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;

/**
 * Loads configuration from pipeline YAML files.
 * This phase handles configuration-specific tasks that come after discovery.
 */
public class ConfigurationLoadingPhase implements PipelineCompilationPhase {

    /**
     * Creates a new ConfigurationLoadingPhase.
     */
    public ConfigurationLoadingPhase() {
    }

    @Override
    public String name() {
        return "Configuration Loading Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // This phase now focuses on configuration tasks that build upon the discovery phase
        // The actual discovery of aspects, templates, and transport mode is handled by PipelineDiscoveryPhase
    }
}
