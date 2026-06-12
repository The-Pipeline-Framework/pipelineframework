package org.pipelineframework.orchestrator.worker;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
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
 * @param artifactId hosted deployable artifact id, when configured
 * @param artifactDigest hosted deployable artifact digest, when configured
 * @param payloadEncodings supported transition payload encodings
 * @param workerProtocols supported transition worker protocols
 */
public record PipelineWorkerCapability(
    String protocolVersion,
    String providerName,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String artifactId,
    String artifactDigest,
    List<String> payloadEncodings,
    List<String> workerProtocols
) {
    public static final String PROTOCOL_VERSION = "1";

    public PipelineWorkerCapability(
        String protocolVersion,
        String providerName,
        String pipelineId,
        String releaseVersion,
        String artifactDigest,
        List<String> payloadEncodings,
        List<String> workerProtocols
    ) {
        this(
            protocolVersion,
            providerName,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            "",
            artifactDigest,
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
            ? contractVersion
            : releaseVersion;
        artifactId = artifactId == null ? "" : artifactId;
        artifactDigest = artifactDigest == null ? "" : artifactDigest;
        payloadEncodings = payloadEncodings == null ? List.of() : List.copyOf(payloadEncodings);
        workerProtocols = workerProtocols == null ? List.of() : List.copyOf(workerProtocols);
    }
}
