package org.pipelineframework.processor.representation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.OperationRepresentationRole;
import org.pipelineframework.representation.spi.ResolvedOperationRepresentation;

class ResolvedOperationRepresentationTest {
    @Test
    void rejectsGenerationKeysThatCollideAfterNormalisation() {
        Map<String, Object> configuration = new LinkedHashMap<>();
        configuration.put("schema", null);
        configuration.put(" schema ", "two");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new ResolvedOperationRepresentation("http", "proof:request",
                OperationRepresentationRole.REQUEST, "http.request",
                new CanonicalType("Input", "example.Input", CanonicalTypeShape.RECORD),
                "GENERATED", Optional.of("example.HttpInput"), Optional.of("example.HttpInputMapper"),
                "1".repeat(64), configuration));

        assertTrue(failure.getMessage().contains("duplicate operation generation configuration key"));
    }
}
