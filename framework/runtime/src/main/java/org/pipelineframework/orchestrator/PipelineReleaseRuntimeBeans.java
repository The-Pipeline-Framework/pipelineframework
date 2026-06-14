package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.FileBackedPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.DynamoPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import org.pipelineframework.orchestrator.worker.DynamoPipelineWorkerRegistry;
import org.pipelineframework.orchestrator.worker.InMemoryPipelineWorkerRegistry;
import org.pipelineframework.orchestrator.worker.PipelineWorkerRegistry;
import java.nio.file.Path;
import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Selects local/dev release runtime collaborators from orchestrator config.
 */
@ApplicationScoped
public class PipelineReleaseRuntimeBeans {

    public static final String DEFAULT_RELEASE_STORAGE_ROOT = "target/tpf-releases";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    Instance<S3Client> s3Clients;

    @Produces
    @ApplicationScoped
    PipelineReleaseArtifactStore pipelineReleaseArtifactStore() {
        String provider = orchestratorConfig == null
            || orchestratorConfig.releases() == null
            || orchestratorConfig.releases().storage() == null
                ? "local"
                : orchestratorConfig.releases().storage().provider();
        if (provider == null || provider.isBlank() || "local".equalsIgnoreCase(provider)) {
            return new LocalPipelineReleaseArtifactStore(storageRoot());
        }
        if ("s3".equalsIgnoreCase(provider)) {
            return new S3PipelineReleaseArtifactStore(
                s3Clients.get(),
                S3PipelineReleaseArtifactStore.bucket(orchestratorConfig),
                S3PipelineReleaseArtifactStore.prefix(orchestratorConfig));
        }
        throw new IllegalStateException(
            "Unsupported pipeline.orchestrator.releases.storage.provider '" + provider + "'");
    }

    @Produces
    @ApplicationScoped
    S3Client s3Client() {
        return S3PipelineReleaseArtifactStore.newClient(orchestratorConfig);
    }

    void closeS3Client(@Disposes S3Client client) {
        if (client != null) {
            client.close();
        }
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

    @Produces
    @ApplicationScoped
    PipelineWorkerRegistry pipelineWorkerRegistry() {
        String provider = orchestratorConfig == null
            || orchestratorConfig.worker() == null
            || orchestratorConfig.worker().lifecycle() == null
                ? "memory"
                : orchestratorConfig.worker().lifecycle().provider();
        if (provider == null || provider.isBlank() || "memory".equalsIgnoreCase(provider)) {
            return new InMemoryPipelineWorkerRegistry();
        }
        if ("dynamo".equalsIgnoreCase(provider)) {
            return new DynamoPipelineWorkerRegistry(orchestratorConfig);
        }
        throw new IllegalStateException(
            "Unsupported pipeline.orchestrator.worker.lifecycle.provider '" + provider + "'");
    }

    public static Duration workerStaleAfter(PipelineOrchestratorConfig orchestratorConfig) {
        if (orchestratorConfig == null
            || orchestratorConfig.worker() == null
            || orchestratorConfig.worker().lifecycle() == null
            || orchestratorConfig.worker().lifecycle().staleAfter() == null) {
            return Duration.ofMinutes(2);
        }
        return orchestratorConfig.worker().lifecycle().staleAfter();
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
