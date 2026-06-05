package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * Versioned metadata for a generated pipeline bundle.
 *
 * @param schemaVersion manifest schema version
 * @param pipelineId logical pipeline id
 * @param bundleVersionId versioned bundle id
 * @param bundleHash deterministic content hash
 * @param platform deployment platform
 * @param transport transport mode
 * @param module module name
 * @param pluginHost whether this module is a plugin host
 * @param runtimeLayout runtime layout, when known
 * @param steps ordered step descriptors
 * @param capabilities declared runtime capabilities
 */
public record PipelineBundleManifest(
    int schemaVersion,
    String pipelineId,
    String bundleVersionId,
    String bundleHash,
    String platform,
    String transport,
    String module,
    boolean pluginHost,
    String runtimeLayout,
    List<PipelineBundleStepDescriptor> steps,
    PipelineBundleCapabilities capabilities
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String RESOURCE_PATH = "META-INF/pipeline/bundle-manifest.json";
    public static final String DEFAULT_PIPELINE_ID = "local-pipeline";
    public static final String DEFAULT_BUNDLE_VERSION_ID = "local-bundle";

    public PipelineBundleManifest {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                "Unsupported pipeline bundle manifest schemaVersion " + schemaVersion);
        }
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
        Objects.requireNonNull(bundleHash, "bundleHash");
        steps = steps == null ? List.of() : List.copyOf(steps);
        capabilities = capabilities == null ? PipelineBundleCapabilities.defaults() : capabilities;
    }

    /**
     * Returns fallback identity used by non-generated tests and legacy artifacts.
     *
     * @return local fallback manifest
     */
    public static PipelineBundleManifest localFallback() {
        return new PipelineBundleManifest(
            CURRENT_SCHEMA_VERSION,
            DEFAULT_PIPELINE_ID,
            DEFAULT_BUNDLE_VERSION_ID,
            DEFAULT_BUNDLE_VERSION_ID,
            null,
            null,
            null,
            false,
            null,
            List.of(),
            PipelineBundleCapabilities.defaults());
    }
}
