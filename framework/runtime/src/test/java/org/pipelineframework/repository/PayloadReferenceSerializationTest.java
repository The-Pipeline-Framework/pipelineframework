package org.pipelineframework.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorPayloadOrigin;
import org.pipelineframework.connector.ConnectorProviderId;

class PayloadReferenceSerializationTest {
    @Test
    void readsLegacyRepositoryJsonWithoutConnectorOrigin() throws Exception {
        String legacy = """
            {"provider":"filesystem","container":"payloads","key":"document","contentType":"text/plain",
             "codec":"string","checksum":"abc","sizeBytes":3,"version":"v1","metadata":{"field":"text"}}
            """;

        PayloadReference reference = PipelineJson.mapper().readValue(legacy, PayloadReference.class);

        assertTrue(reference.connectorOrigin().isEmpty());
        assertEquals("document", reference.key());
        assertEquals(reference, PipelineJson.mapper().readValue(
            PipelineJson.mapper().writeValueAsString(reference), PayloadReference.class));
    }

    @Test
    void roundTripsBindingAndSanitizedConfigurationProvenance() throws Exception {
        ConnectorPayloadOrigin origin = new ConnectorPayloadOrigin(
            ConnectorBindingName.of("documents"),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("filesystem.objects"), "filesystem", ConnectorOperationKind.OBJECT_SOURCE, 1),
            1,
            Optional.of(new ConnectorConfigurationSnapshot(
                "filesystem.objects.config", 1, "digest", List.of(new ConnectionRef("documents-volume")))));
        PayloadReference reference = new PayloadReference(
            "filesystem", "/documents", "document.txt", "text/plain", "raw", "abc", 3, "v1", Map.of(),
            Optional.of(origin));

        String json = PipelineJson.mapper().writeValueAsString(reference);
        PayloadReference decoded = PipelineJson.mapper().readValue(json, PayloadReference.class);

        assertEquals(reference, decoded);
        assertTrue(json.contains("documents-volume"));
        assertTrue(!json.contains("resolved"));
    }
}
