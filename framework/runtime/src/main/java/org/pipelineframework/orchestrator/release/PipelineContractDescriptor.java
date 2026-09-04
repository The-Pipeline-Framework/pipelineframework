package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.composition.PipelineCompositionDescriptor;

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
    String canonicalCatalogFingerprint,
    PipelineCompositionDescriptor composition,
    List<ImportedPipelineDefinitionDescriptor> importedDefinitions
) {
    public static final int CURRENT_SCHEMA_VERSION = 3;
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
        composition = composition == null ? PipelineCompositionDescriptor.empty() : composition;
        importedDefinitions = importedDefinitions == null ? List.of() : List.copyOf(importedDefinitions);
        if (schemaVersion < 3 && composition.present()) {
            throw new IllegalArgumentException("Pipeline composition requires contract schemaVersion 3");
        }
        if (schemaVersion < 3 && !importedDefinitions.isEmpty()) {
            throw new IllegalArgumentException("Imported pipeline definitions require contract schemaVersion 3");
        }
    }

    /** Schema-v1/v2 source compatibility; earlier contracts have no composition descriptor. */
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
            runtimeLayout, steps, capabilities, Map.of(), "", PipelineCompositionDescriptor.empty(), List.of());
    }

    /** Schema-v2 source compatibility; v2 contracts have no composition descriptor. */
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
        PipelineBundleCapabilities capabilities,
        Map<String, Map<String, Object>> canonicalTypes,
        String canonicalCatalogFingerprint
    ) {
        this(schemaVersion, pipelineId, contractVersion, contractHash, platform, transport, module, pluginHost,
            runtimeLayout, steps, capabilities, canonicalTypes, canonicalCatalogFingerprint,
            PipelineCompositionDescriptor.empty(), List.of());
    }

    /** Source compatibility for schema-v3 contracts created before imported definition provenance was added. */
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
        PipelineBundleCapabilities capabilities,
        Map<String, Map<String, Object>> canonicalTypes,
        String canonicalCatalogFingerprint,
        PipelineCompositionDescriptor composition
    ) {
        this(schemaVersion, pipelineId, contractVersion, contractHash, platform, transport, module, pluginHost,
            runtimeLayout, steps, capabilities, canonicalTypes, canonicalCatalogFingerprint, composition, List.of());
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
            "",
            PipelineCompositionDescriptor.empty(),
            List.of());
    }
}
