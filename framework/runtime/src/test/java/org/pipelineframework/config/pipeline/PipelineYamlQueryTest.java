package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PipelineYamlQueryTest {

    @Test
    void constructsQueryWithRequiredFields() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "customer-risk-by-id",
            "customer-risk",
            "com.example.CustomerRiskLookup",
            "com.example.CustomerRiskSnapshot",
            "v1",
            Map.of("source", "risk-db"));

        assertEquals("customer-risk-by-id", query.id());
        assertEquals("customer-risk", query.connector());
        assertEquals("com.example.CustomerRiskLookup", query.inputType());
        assertEquals("com.example.CustomerRiskSnapshot", query.outputType());
        assertEquals("v1", query.version());
        assertEquals(Map.of("source", "risk-db"), query.config());
    }

    @Test
    void rejectsNullId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery(null, "connector", "in.Type", "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsBlankId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("   ", "connector", "in.Type", "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsNullConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", null, "in.Type", "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsBlankConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "", "in.Type", "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsNullInputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "connector", null, "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsBlankInputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "connector", "  ", "out.Type", "v1", Map.of()));
    }

    @Test
    void rejectsNullOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "connector", "in.Type", null, "v1", Map.of()));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "connector", "in.Type", "", "v1", Map.of()));
    }

    @Test
    void defaultsVersionToV1WhenNull() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "connector", "in.Type", "out.Type", null, Map.of());

        assertEquals("v1", query.version());
    }

    @Test
    void defaultsVersionToV1WhenBlank() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "connector", "in.Type", "out.Type", "  ", Map.of());

        assertEquals("v1", query.version());
    }

    @Test
    void defaultsConfigToEmptyMapWhenNull() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "connector", "in.Type", "out.Type", "v1", null);

        assertEquals(Map.of(), query.config());
    }

    @Test
    void makesImmutableCopyOfConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("key", "value");
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "connector", "in.Type", "out.Type", "v1", config);

        config.put("extra", "extra-value");

        assertEquals(1, query.config().size());
        assertThrows(UnsupportedOperationException.class, () -> query.config().put("k", "v"));
    }

    @Test
    void retainsCustomVersion() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "connector", "in.Type", "out.Type", "v2", Map.of());

        assertEquals("v2", query.version());
    }
}