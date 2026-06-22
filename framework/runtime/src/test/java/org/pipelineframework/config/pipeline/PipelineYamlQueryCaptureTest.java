package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class PipelineYamlQueryCaptureTest {

    @Test
    void constructsWithKeyFields() {
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(List.of("customerId", "tenantId"));

        assertEquals(List.of("customerId", "tenantId"), capture.keyFields());
    }

    @Test
    void defaultsKeyFieldsToEmptyListWhenNull() {
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(null);

        assertTrue(capture.keyFields().isEmpty());
    }

    @Test
    void makesImmutableCopyOfKeyFields() {
        List<String> fields = new ArrayList<>();
        fields.add("customerId");
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(fields);

        fields.add("tenantId");

        assertEquals(1, capture.keyFields().size());
        assertThrows(UnsupportedOperationException.class, () -> capture.keyFields().add("x"));
    }

    @Test
    void acceptsEmptyKeyFields() {
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(List.of());

        assertTrue(capture.keyFields().isEmpty());
    }

    @Test
    void acceptsMultipleKeyFields() {
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(
            List.of("customerId", "orderId", "productId"));

        assertEquals(3, capture.keyFields().size());
        assertEquals("customerId", capture.keyFields().get(0));
        assertEquals("orderId", capture.keyFields().get(1));
        assertEquals("productId", capture.keyFields().get(2));
    }
}