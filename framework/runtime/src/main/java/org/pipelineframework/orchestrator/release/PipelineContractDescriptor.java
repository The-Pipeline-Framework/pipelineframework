package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;

/**
 * Generated semantic pipeline contract emitted at build time.
 */
public record PipelineContractDescriptor(
    int schemaVersion,
    String pipelineId,
    String contractVersion,
    String contractHash,
    String platform,
    String transport,
    String module,
    boolean pluginHost,
    String runtimeLayout,
    List<PipelineBundleStepDescriptor> steps,
    PipelineBundleCapabilities capabilities,
    Map<String, Map<String, Object>> canonicalTypes,
    String canonicalCatalogFingerprint
) {
    public static final int CURRENT_SCHEMA_VERSION = 2;
    public static final String RESOURCE_PATH = "META-INF/pipeline/pipeline-contract.json";
    public static final String DEFAULT_PIPELINE_ID = "local-pipeline";
    public static final String DEFAULT_CONTRACT_VERSION = "local-contract";
    public static final String DEFAULT_CONTRACT_HASH = "local-contract-hash";

    public PipelineContractDescriptor {
        if (schemaVersion < 1 || schemaVersion > CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported pipeline contract schemaVersion " + schemaVersion);
        }
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(contractHash, "contractHash");
        steps = steps == null ? List.of() : List.copyOf(steps);
        capabilities = capabilities == null ? PipelineBundleCapabilities.defaults() : capabilities;
        canonicalTypes = canonicalTypes == null ? Map.of() : canonicalTypes.entrySet().stream()
            .collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> Map.copyOf(entry.getValue())));
        canonicalCatalogFingerprint = canonicalCatalogFingerprint == null ? "" : canonicalCatalogFingerprint;
    }

    /** Schema-v1 source compatibility; v1 contracts have no canonical binding catalog. */
    public PipelineContractDescriptor(
        int schemaVersion,
        String pipelineId,
        String contractVersion,
        String contractHash,
        String platform,
        String transport,
        String module,
        boolean pluginHost,
        String runtimeLayout,
        List<PipelineBundleStepDescriptor> steps,
        PipelineBundleCapabilities capabilities
    ) {
        this(schemaVersion, pipelineId, contractVersion, contractHash, platform, transport, module, pluginHost,
            runtimeLayout, steps, capabilities, Map.of(), "");
    }

    public static PipelineContractDescriptor localFallback() {
        return new PipelineContractDescriptor(
            CURRENT_SCHEMA_VERSION,
            DEFAULT_PIPELINE_ID,
            DEFAULT_CONTRACT_VERSION,
            DEFAULT_CONTRACT_HASH,
            null,
            null,
            null,
            false,
            null,
            List.of(),
            PipelineBundleCapabilities.defaults(),
            Map.of(),
            "");
    }
}
