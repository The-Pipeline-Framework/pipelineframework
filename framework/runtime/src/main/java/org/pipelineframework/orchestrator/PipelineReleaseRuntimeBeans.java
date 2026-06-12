package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.FileBackedPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.DynamoPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import java.nio.file.Path;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/**
 * Selects local/dev release runtime collaborators from orchestrator config.
 */
@ApplicationScoped
public class PipelineReleaseRuntimeBeans {

    public static final String DEFAULT_RELEASE_STORAGE_ROOT = "target/tpf-releases";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Produces
    @ApplicationScoped
    PipelineReleaseArtifactStore pipelineReleaseArtifactStore() {
        return new LocalPipelineReleaseArtifactStore(storageRoot());
    }

    @Produces
    @ApplicationScoped
    PipelineReleaseRegistry pipelineReleaseRegistry() {
        String provider = orchestratorConfig == null
            || orchestratorConfig.releases() == null
            || orchestratorConfig.releases().registry() == null
                ? "memory"
                : orchestratorConfig.releases().registry().provider();
        if (provider == null || provider.isBlank() || "memory".equalsIgnoreCase(provider)) {
            return new InMemoryPipelineReleaseRegistry();
        }
        if ("file".equalsIgnoreCase(provider)) {
            return new FileBackedPipelineReleaseRegistry(storageRoot());
        }
        if ("dynamo".equalsIgnoreCase(provider)) {
            return new DynamoPipelineReleaseRegistry(orchestratorConfig);
        }
        throw new IllegalStateException(
            "Unsupported pipeline.orchestrator.releases.registry.provider '" + provider + "'");
    }

    public static Path storageRoot(PipelineOrchestratorConfig orchestratorConfig) {
        String configured = orchestratorConfig == null
            || orchestratorConfig.releases() == null
            || orchestratorConfig.releases().storage() == null
                ? DEFAULT_RELEASE_STORAGE_ROOT
                : orchestratorConfig.releases().storage().root();
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("pipeline.orchestrator.releases.storage.root must not be blank");
        }
        return Path.of(configured);
    }

    private Path storageRoot() {
        return storageRoot(orchestratorConfig);
    }
}
