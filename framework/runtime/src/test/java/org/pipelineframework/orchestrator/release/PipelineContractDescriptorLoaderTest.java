package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class PipelineContractDescriptorLoaderTest {

    @Test
    void readsGeneratedResumableSourceContinuationMetadata() {
        String json = """
            {
              "schemaVersion": 2,
              "pipelineId": "csv-payments",
              "contractVersion": "1",
              "contractHash": "hash",
              "steps": [],
              "capabilities": {},
              "canonicalTypes": {},
              "canonicalCatalogFingerprint": "catalog",
              "resumableSourceContinuations": [{
                "producerStepIndex": 0,
                "awaitStepIndex": 1,
                "terminalScalarSuffix": true,
                "capabilities": ["RESUMABLE_SOURCE"]
              }]
            }
            """;

        PipelineContractDescriptor contract = new PipelineContractDescriptorLoader().load(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, contract.resumableSourceContinuations().size());
        assertEquals(0, contract.resumableSourceContinuations().getFirst().get("producerStepIndex"));
        assertTrue((Boolean) contract.resumableSourceContinuations().getFirst().get("terminalScalarSuffix"));
    }
}
