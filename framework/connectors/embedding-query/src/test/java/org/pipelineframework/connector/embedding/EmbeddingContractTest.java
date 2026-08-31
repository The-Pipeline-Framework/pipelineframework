package org.pipelineframework.connector.embedding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class EmbeddingContractTest {
    @Test
    void ownsTypedImmutableFloatVectorAndCacheableOperationIdentity() {
        ArrayList<Float> mutable = new ArrayList<>(List.of(1.0f, -0.5f, 1.0f));
        EmbeddingResult result = new EmbeddingResult("item", "text", mutable);
        mutable.clear();

        assertEquals(List.of(1.0f, -0.5f, 1.0f), result.values());
        assertEquals(" item ", new EmbeddingRequest(" item ", " text ").itemId());
        assertEquals(" text ", new EmbeddingRequest(" item ", " text ").text());
        assertThrows(UnsupportedOperationException.class, () -> result.values().add(2.0f));
        assertThrows(IllegalArgumentException.class,
            () -> new EmbeddingResult("item", "text", List.of(Float.NaN)));
        assertEquals("embed", new TestOperation().id());
        assertEquals(QueryCapabilities.cacheable(), new TestOperation().capabilities());
    }

    @Test
    void registersRepeatedFloat32CanonicalVocabulary() {
        ProtocolTypeContributor contributor = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(EmbeddingProtocolTypeContributor.class::isInstance)
            .findFirst().orElseThrow();
        var result = contributor.protocolTypes().stream()
            .filter(type -> type.identity().equals(EmbeddingProtocolTypeContributor.RESULT))
            .findFirst().orElseThrow();
        var fields = ((PipelineTemplateTypeDefinition.RecordType) result.definition()).fields();

        assertEquals("float32", fields.get(2).type().name());
        assertTrue(fields.get(2).repeated());
    }

    private static class TestOperation implements EmbeddingQueryOperation {
        @Override
        public CompletionStage<QueryOutcome<EmbeddingResult>> query(
            QueryInvocation<EmbeddingRequest, org.pipelineframework.connector.ConnectorConfigurationDocument,
                EmbeddingResult> invocation
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
