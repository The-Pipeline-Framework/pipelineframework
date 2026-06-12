package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptorLoader;

/**
 * Resolves the local worker identity from generated contract metadata and deployment config.
 */
@ApplicationScoped
public class PipelineReleaseIdentityResolver {

    @Inject
    PipelineContractDescriptorLoader contractLoader;

    private volatile Optional<PipelineContractDescriptor> cachedContract;

    public String pipelineId(PipelineOrchestratorConfig config) {
        String configured = config == null ? null : config.pipelineId();
        if (isExplicit(configured, PipelineContractDescriptor.DEFAULT_PIPELINE_ID)) {
            return configured.trim();
        }
        return contract().pipelineId();
    }

    public String contractVersion() {
        return contract().contractVersion();
    }

    public String releaseVersion(PipelineOrchestratorConfig config) {
        String configured = config == null ? null : config.releaseVersion();
        if (isExplicit(configured, PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION)) {
            return configured.trim();
        }
        return contractVersion();
    }

    public String artifactId(PipelineOrchestratorConfig config) {
        return config == null || config.worker() == null
            ? ""
            : config.worker().artifactId().orElse("").trim();
    }

    public String artifactDigest(PipelineOrchestratorConfig config) {
        return config == null || config.worker() == null
            ? ""
            : config.worker().artifactDigest().orElse("").trim();
    }

    public PipelineBundleCapabilities capabilities() {
        return contract().capabilities();
    }

    public Optional<String> validateCommandIdentity(
        TransitionCommandEnvelope command,
        PipelineOrchestratorConfig config) {
        Objects.requireNonNull(command, "command");
        String expectedPipelineId = pipelineId(config);
        String expectedContractVersion = contractVersion();
        String expectedReleaseVersion = releaseVersion(config);
        if (!expectedPipelineId.equals(command.pipelineId())
            || !expectedContractVersion.equals(command.contractVersion())
            || !expectedReleaseVersion.equals(command.releaseVersion())) {
            return Optional.of(
                "Transition command targets pipelineId="
                    + command.pipelineId()
                    + ", contractVersion="
                    + command.contractVersion()
                    + ", releaseVersion="
                    + command.releaseVersion()
                    + " but local worker is pipelineId="
                    + expectedPipelineId
                    + ", contractVersion="
                    + expectedContractVersion
                    + ", releaseVersion="
                    + expectedReleaseVersion);
        }
        return Optional.empty();
    }

    private PipelineContractDescriptor contract() {
        Optional<PipelineContractDescriptor> loaded = cachedContract;
        if (loaded == null) {
            synchronized (this) {
                loaded = cachedContract;
                if (loaded == null) {
                    loaded = contractLoader().load();
                    cachedContract = loaded;
                }
            }
        }
        return loaded.orElseGet(PipelineContractDescriptor::localFallback);
    }

    private PipelineContractDescriptorLoader contractLoader() {
        return contractLoader == null ? new PipelineContractDescriptorLoader() : contractLoader;
    }

    private static boolean isExplicit(String configured, String defaultValue) {
        return configured != null
            && !configured.isBlank()
            && !defaultValue.equals(configured.trim());
    }
}
