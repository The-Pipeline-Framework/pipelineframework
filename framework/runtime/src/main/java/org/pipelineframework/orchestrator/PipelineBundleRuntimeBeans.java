package org.pipelineframework.orchestrator;

import java.nio.file.Path;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/**
 * Selects local/dev hosted bundle runtime collaborators from orchestrator config.
 */
@ApplicationScoped
public class PipelineBundleRuntimeBeans {

    static final String DEFAULT_BUNDLE_STORAGE_ROOT = "target/tpf-bundles";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineBundleManifestLoader manifestLoader;

    @Produces
    @ApplicationScoped
    PipelineBundleArtifactStore pipelineBundleArtifactStore() {
        return new LocalPipelineBundleArtifactStore(storageRoot(), manifestLoader);
    }

    @Produces
    @ApplicationScoped
    PipelineBundleRegistry pipelineBundleRegistry() {
        String provider = orchestratorConfig == null
            || orchestratorConfig.bundles() == null
            || orchestratorConfig.bundles().registry() == null
                ? "memory"
                : orchestratorConfig.bundles().registry().provider();
        if (provider == null || provider.isBlank() || "memory".equalsIgnoreCase(provider)) {
            return new InMemoryPipelineBundleRegistry();
        }
        if ("file".equalsIgnoreCase(provider)) {
            return new FileBackedPipelineBundleRegistry(storageRoot());
        }
        throw new IllegalStateException(
            "Unsupported pipeline.orchestrator.bundles.registry.provider '" + provider + "'");
    }

    static Path storageRoot(PipelineOrchestratorConfig orchestratorConfig) {
        String configured = orchestratorConfig == null
            || orchestratorConfig.bundles() == null
            || orchestratorConfig.bundles().storage() == null
                ? DEFAULT_BUNDLE_STORAGE_ROOT
                : orchestratorConfig.bundles().storage().root();
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("pipeline.orchestrator.bundles.storage.root must not be blank");
        }
        return Path.of(configured);
    }

    private Path storageRoot() {
        return storageRoot(orchestratorConfig);
    }
}
