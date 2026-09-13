package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PipelineYamlConfigQueriesTest {

    @Test
    void constructsConfigWithQueriesMap() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "customer-risk-by-id",
            "jpa",
            "com.example.CustomerRiskLookup",
            "com.example.CustomerRiskSnapshot",
            "v1",
            jpa());

        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of("customer-risk-by-id", query),
            List.of(),
            null,
            null);

        assertEquals(1, config.queries().size());
        assertNotNull(config.queries().get("customer-risk-by-id"));
        assertEquals("jpa", config.queries().get("customer-risk-by-id").connector());
    }

    @Test
    void defaultsQueriesToEmptyMapWhenNull() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            null,
            List.of(),
            null,
            null);

        assertNotNull(config.queries());
        assertTrue(config.queries().isEmpty());
    }

    @Test
    void rejectsQueriesMapWithNullKey() {
        Map<String, PipelineYamlQuery> queriesWithNullKey = new HashMap<>();
        queriesWithNullKey.put(null, new PipelineYamlQuery(
            "id", "jpa", "in.Type", "out.Type", "v1", jpa()));

        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfig(
                "com.example", "GRPC", "COMPUTE",
                List.of(), Map.of(),
                queriesWithNullKey,
                List.of(), null, null));
    }

    @Test
    void rejectsQueriesMapWithNullValue() {
        Map<String, PipelineYamlQuery> queriesWithNullValue = new HashMap<>();
        queriesWithNullValue.put("customer-risk-by-id", null);

        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlConfig(
                "com.example", "GRPC", "COMPUTE",
                List.of(), Map.of(),
                queriesWithNullValue,
                List.of(), null, null));
    }

    @Test
    void queriesMapIsImmutable() {
        Map<String, PipelineYamlQuery> queries = new HashMap<>();
        queries.put("q1", new PipelineYamlQuery("q1", "jpa", "in.T", "out.T", "v1", jpa()));

        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example", "GRPC", "COMPUTE",
            List.of(), Map.of(),
            queries,
            List.of(), null, null);

        assertThrows(UnsupportedOperationException.class, () -> config.queries().put("extra", null));
    }

    @Test
    void backwardCompatibleConstructorDefaultsQueriesToEmpty() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example",
            "GRPC",
            List.of(),
            List.of());

        assertNotNull(config.queries());
        assertTrue(config.queries().isEmpty());
    }

    @Test
    void withTransportPreservesQueries() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "q1", "jpa", "in.Type", "out.Type", "v1", jpa());

        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example", "GRPC", "COMPUTE",
            List.of(), Map.of(),
            Map.of("q1", query),
            List.of(), null, null);

        PipelineYamlConfig updated = config.withTransport("REST");

        assertEquals("REST", updated.transport());
        assertEquals(1, updated.queries().size());
        assertNotNull(updated.queries().get("q1"));
    }

    @Test
    void withPlatformPreservesQueries() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "q1", "jpa", "in.Type", "out.Type", "v1", jpa());

        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example", "GRPC", "COMPUTE",
            List.of(), Map.of(),
            Map.of("q1", query),
            List.of(), null, null);

        PipelineYamlConfig updated = config.withPlatform("FUNCTION");

        assertEquals("FUNCTION", updated.platform());
        assertEquals(1, updated.queries().size());
    }

    @Test
    void sourcesBackwardCompatibleConstructorDefaultsQueriesToEmpty() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "com.example",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            List.of(),
            null,
            null);

        assertNotNull(config.queries());
        assertTrue(config.queries().isEmpty());
    }

    private static PipelineYamlJpaQuery jpa() {
        return new PipelineYamlJpaQuery(
            "com.example.Entity",
            Map.of("id", "input.id"),
            Map.of(),
            "single");
    }
}
