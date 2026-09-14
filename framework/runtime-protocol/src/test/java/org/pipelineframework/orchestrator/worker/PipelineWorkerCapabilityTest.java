package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

class PipelineWorkerCapabilityTest {

    @Test
    void normalizesReleaseIdentityAndCopiesProtocolLists() {
        List<String> encodings = new ArrayList<>(List.of("application/json"));
        List<String> protocols = new ArrayList<>(List.of("rest"));

        PipelineWorkerCapability capability = new PipelineWorkerCapability(
            PipelineWorkerCapability.PROTOCOL_VERSION,
            "rest",
            "orders",
            "",
            "",
            null,
            null,
            encodings,
            protocols);
        encodings.clear();
        protocols.clear();

        assertEquals(PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION, capability.contractVersion());
        assertEquals(capability.contractVersion(), capability.releaseVersion());
        assertEquals(List.of("application/json"), capability.payloadEncodings());
        assertEquals(List.of("rest"), capability.workerProtocols());
        assertThrows(UnsupportedOperationException.class, () -> capability.workerProtocols().clear());
    }
}
