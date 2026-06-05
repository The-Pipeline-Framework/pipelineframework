package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * Identity and protocol capabilities advertised by a transition worker.
 *
 * @param protocolVersion capability protocol version
 * @param providerName worker provider name
 * @param pipelineId hosted pipeline id
 * @param bundleVersionId hosted bundle version id
 * @param bundleHash generated bundle hash when available
 * @param payloadEncodings supported transition payload encodings
 * @param workerProtocols supported transition worker protocols
 */
public record PipelineWorkerCapability(
    String protocolVersion,
    String providerName,
    String pipelineId,
    String bundleVersionId,
    String bundleHash,
    List<String> payloadEncodings,
    List<String> workerProtocols
) {
    public static final String PROTOCOL_VERSION = "1";

    public PipelineWorkerCapability {
        Objects.requireNonNull(protocolVersion, "protocolVersion");
        Objects.requireNonNull(providerName, "providerName");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
        bundleHash = bundleHash == null ? "" : bundleHash;
        payloadEncodings = payloadEncodings == null ? List.of() : List.copyOf(payloadEncodings);
        workerProtocols = workerProtocols == null ? List.of() : List.copyOf(workerProtocols);
    }
}
