package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Resolves effective transition bundle identity from generated metadata and runtime overrides.
 */
@ApplicationScoped
public class PipelineBundleIdentityResolver {

    @Inject
    PipelineBundleManifestLoader manifestLoader;

    private volatile Optional<PipelineBundleManifest> cachedManifest;

    /**
     * Resolve the effective local pipeline id.
     *
     * @param config orchestrator config
     * @return effective pipeline id
     */
    public String pipelineId(PipelineOrchestratorConfig config) {
        String configured = config == null ? null : config.pipelineId();
        if (isExplicitPipelineOverride(configured)) {
            return configured.trim();
        }
        return manifest().pipelineId();
    }

    /**
     * Resolve the effective local bundle version id.
     *
     * @param config orchestrator config
     * @return effective bundle version id
     */
    public String bundleVersionId(PipelineOrchestratorConfig config) {
        String configured = config == null ? null : config.bundleVersionId();
        if (isExplicitBundleOverride(configured)) {
            return configured.trim();
        }
        return manifest().bundleVersionId();
    }

    /**
     * Resolve the generated local bundle hash.
     *
     * @return bundle hash from generated metadata
     */
    public String bundleHash() {
        return manifest().bundleHash();
    }

    /**
     * Resolve generated local bundle capabilities.
     *
     * @return generated bundle capabilities
     */
    public PipelineBundleCapabilities capabilities() {
        return manifest().capabilities();
    }

    /**
     * Validates a command envelope against the effective local bundle identity.
     *
     * @param command transition command envelope
     * @param config orchestrator config
     * @return mismatch description, or empty when the identity matches
     */
    public Optional<String> validateCommandIdentity(
        TransitionCommandEnvelope command,
        PipelineOrchestratorConfig config) {
        Objects.requireNonNull(command, "command");
        String expectedPipelineId = pipelineId(config);
        String expectedBundleVersionId = bundleVersionId(config);
        if (!expectedPipelineId.equals(command.pipelineId()) || !expectedBundleVersionId.equals(command.bundleVersionId())) {
            return Optional.of(
                "Transition command targets pipelineId="
                    + command.pipelineId()
                    + ", bundleVersionId="
                    + command.bundleVersionId()
                    + " but local worker is pipelineId="
                    + expectedPipelineId
                    + ", bundleVersionId="
                    + expectedBundleVersionId);
        }
        return Optional.empty();
    }

    private PipelineBundleManifest manifest() {
        Optional<PipelineBundleManifest> loaded = cachedManifest;
        if (loaded == null) {
            synchronized (this) {
                loaded = cachedManifest;
                if (loaded == null) {
                    loaded = loader().load();
                    cachedManifest = loaded;
                }
            }
        }
        return loaded.orElseGet(PipelineBundleManifest::localFallback);
    }

    private PipelineBundleManifestLoader loader() {
        return manifestLoader == null ? new PipelineBundleManifestLoader() : manifestLoader;
    }

    private static boolean isExplicitPipelineOverride(String configured) {
        return configured != null
            && !configured.isBlank()
            && !PipelineBundleManifest.DEFAULT_PIPELINE_ID.equals(configured.trim());
    }

    private static boolean isExplicitBundleOverride(String configured) {
        return configured != null
            && !configured.isBlank()
            && !PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID.equals(configured.trim());
    }
}
