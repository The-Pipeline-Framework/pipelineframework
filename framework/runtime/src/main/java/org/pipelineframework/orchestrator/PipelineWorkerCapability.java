package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * Identity and protocol capabilities advertised by a transition worker.
 *
 * @param protocolVersion capability protocol version
 * @param providerName worker provider name
 * @param pipelineId hosted pipeline id
 * @param contractVersion hosted contract version
 * @param releaseVersion hosted release version
 * @param bundleVersionId hosted bundle version id
 * @param bundleHash generated bundle hash when available
 * @param payloadEncodings supported transition payload encodings
 * @param workerProtocols supported transition worker protocols
 */
public record PipelineWorkerCapability(
    String protocolVersion,
    String providerName,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String bundleVersionId,
    String bundleHash,
    List<String> payloadEncodings,
    List<String> workerProtocols
) {
    public static final String PROTOCOL_VERSION = "1";

    public PipelineWorkerCapability(
        String protocolVersion,
        String providerName,
        String pipelineId,
        String bundleVersionId,
        String bundleHash,
        List<String> payloadEncodings,
        List<String> workerProtocols
    ) {
        this(
            protocolVersion,
            providerName,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            bundleVersionId,
            bundleVersionId,
            bundleHash,
            payloadEncodings,
            workerProtocols);
    }

    public PipelineWorkerCapability {
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(providerName, "providerName");
        Objects.requireNonNull(pipelineId, "pipelineId");
        contractVersion = contractVersion == null || contractVersion.isBlank()
            ? PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION
            : contractVersion;
        releaseVersion = releaseVersion == null || releaseVersion.isBlank()
            ? bundleVersionId
            : releaseVersion;
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
        bundleHash = bundleHash == null ? "" : bundleHash;
        payloadEncodings = payloadEncodings == null ? List.of() : List.copyOf(payloadEncodings);
        workerProtocols = workerProtocols == null ? List.of() : List.copyOf(workerProtocols);
    }
}
