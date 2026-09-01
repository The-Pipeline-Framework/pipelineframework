package org.pipelineframework.connector.vector.pgvector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.vector.VectorUpsertRequest;

class PgVectorConnectorTest {
    @Test void fingerprintsFrameFieldsAndRawFloatBits() {
        var first = new VectorUpsertRequest("ab", "c", List.of(1.0f, -0.0f));
        var second = new VectorUpsertRequest("a", "bc", List.of(1.0f, 0.0f));
        assertNotEquals(PgVectorRequestFingerprint.of(first), PgVectorRequestFingerprint.of(second));
        assertEquals("[1.0,-0.0]", PgVectorConnector.vectorLiteral(first.values()));
        assertEquals(first.values(), PgVectorConnector.parseVector("[1.0,-0.0]"));
    }

    @Test void validatesDeploymentOwnedIdentifiers() {
        assertThrows(IllegalArgumentException.class,
            () -> PgVectorConnector.runtimeSettings("public", "rag_vectors;drop", "commands"));
    }
}
