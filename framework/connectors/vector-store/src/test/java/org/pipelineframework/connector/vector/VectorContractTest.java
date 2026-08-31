package org.pipelineframework.connector.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class VectorContractTest {
    @Test
    void validatesPortableRequestsAndKeepsRepeatedValuesImmutable() {
        ArrayList<Float> mutable = new ArrayList<>(List.of(1.0f, 0.0f, 1.0f));
        VectorUpsertRequest request = new VectorUpsertRequest("item", "content", mutable);
        mutable.clear();

        assertEquals(List.of(1.0f, 0.0f, 1.0f), request.values());
        assertEquals(" item ", new VectorUpsertRequest(" item ", " content ", List.of(1.0f)).itemId());
        assertEquals(" content ", new VectorUpsertRequest(" item ", " content ", List.of(1.0f)).content());
        assertThrows(IllegalArgumentException.class,
            () -> new VectorUpsertRequest("item", "content", List.of(Float.POSITIVE_INFINITY)));
        assertThrows(IllegalArgumentException.class,
            () -> new VectorSearchRequest("query", "text", List.of(1.0f), 0));
        assertEquals(List.of(), new VectorSearchResult("query", "text", List.of()).matches());
    }

    @Test
    void contributesRepeatedFloatsAndRepeatedMatches() {
        ProtocolTypeContributor contributor = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(VectorProtocolTypeContributor.class::isInstance)
            .findFirst().orElseThrow();
        var definitions = contributor.protocolTypes();
        var upsert = definitions.stream().filter(type -> type.identity().equals(VectorProtocolTypeContributor.UPSERT_REQUEST))
            .findFirst().orElseThrow();
        var result = definitions.stream().filter(type -> type.identity().equals(VectorProtocolTypeContributor.SEARCH_RESULT))
            .findFirst().orElseThrow();
        var values = ((PipelineTemplateTypeDefinition.RecordType) upsert.definition()).fields().get(2);
        var matches = ((PipelineTemplateTypeDefinition.RecordType) result.definition()).fields().get(2);

        assertTrue(values.repeated());
        assertEquals("float32", values.type().name());
        assertTrue(matches.repeated());
        assertEquals("tpf.vector.VectorMatch", matches.type().name());
    }
}
