package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Objects;

import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleManifest;
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
    PipelineBundleCapabilities capabilities
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String RESOURCE_PATH = "META-INF/pipeline/pipeline-contract.json";
    public static final String DEFAULT_CONTRACT_VERSION = "local-contract";
    public static final String DEFAULT_CONTRACT_HASH = "local-contract-hash";

    public PipelineContractDescriptor {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported pipeline contract schemaVersion " + schemaVersion);
        }
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(contractHash, "contractHash");
        steps = steps == null ? List.of() : List.copyOf(steps);
        capabilities = capabilities == null ? PipelineBundleCapabilities.defaults() : capabilities;
    }

    public static PipelineContractDescriptor localFallback() {
        return new PipelineContractDescriptor(
            CURRENT_SCHEMA_VERSION,
            PipelineBundleManifest.DEFAULT_PIPELINE_ID,
            DEFAULT_CONTRACT_VERSION,
            DEFAULT_CONTRACT_HASH,
            null,
            null,
            null,
            false,
            null,
            List.of(),
            PipelineBundleCapabilities.defaults());
    }

    public static PipelineContractDescriptor fromManifest(PipelineBundleManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        return new PipelineContractDescriptor(
            CURRENT_SCHEMA_VERSION,
            manifest.pipelineId(),
            "sha256:" + manifest.bundleHash(),
            manifest.bundleHash(),
            manifest.platform(),
            manifest.transport(),
            manifest.module(),
            manifest.pluginHost(),
            manifest.runtimeLayout(),
            manifest.steps(),
            manifest.capabilities());
    }
}
