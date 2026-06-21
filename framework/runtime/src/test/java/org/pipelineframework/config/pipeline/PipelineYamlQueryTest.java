package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

class PipelineYamlQueryTest {

    @Test
    void constructsQueryWithRequiredFields() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "customer-risk-by-id",
            "jpa",
            "com.example.CustomerRiskLookup",
            "com.example.CustomerRiskSnapshot",
            "v1",
            jpa());

        assertEquals("customer-risk-by-id", query.id());
        assertEquals("jpa", query.connector());
        assertEquals("com.example.CustomerRiskLookup", query.inputType());
        assertEquals("com.example.CustomerRiskSnapshot", query.outputType());
        assertEquals("v1", query.version());
        assertEquals("com.example.CustomerRiskEntity", query.jpa().entity());
    }

    @Test
    void rejectsNullId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery(null, "jpa", "in.Type", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsBlankId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("   ", "jpa", "in.Type", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsNullConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", null, "in.Type", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsBlankConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "", "in.Type", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsNonJpaConnector() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "custom", "in.Type", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsNullInputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "jpa", null, "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsBlankInputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "jpa", "  ", "out.Type", "v1", jpa()));
    }

    @Test
    void rejectsNullOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "jpa", "in.Type", null, "v1", jpa()));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "jpa", "in.Type", "", "v1", jpa()));
    }

    @Test
    void defaultsVersionToV1WhenNull() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "jpa", "in.Type", "out.Type", null, jpa());

        assertEquals("v1", query.version());
    }

    @Test
    void defaultsVersionToV1WhenBlank() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "jpa", "in.Type", "out.Type", "  ", jpa());

        assertEquals("v1", query.version());
    }

    @Test
    void rejectsMissingJpaConfig() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlQuery("query-id", "jpa", "in.Type", "out.Type", "v1", null));
    }

    @Test
    void retainsCustomVersion() {
        PipelineYamlQuery query = new PipelineYamlQuery(
            "query-id", "jpa", "in.Type", "out.Type", "v2", jpa());

        assertEquals("v2", query.version());
    }

    @Test
    void validatesJpaConfig() {
        PipelineYamlJpaQuery jpa = new PipelineYamlJpaQuery(
            "com.example.CustomerRiskEntity",
            Map.of("customerId", "input.customerId"),
            Map.of("riskBand", "riskBand"),
            null);

        assertEquals("single", jpa.result());
        assertEquals("input.customerId", jpa.where().get("customerId"));
        assertThrows(UnsupportedOperationException.class, () -> jpa.where().put("other", "input.other"));
    }

    @Test
    void rejectsUnsupportedJpaResult() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlJpaQuery(
                "com.example.CustomerRiskEntity",
                Map.of("customerId", "input.customerId"),
                Map.of(),
                "optional"));
    }

    private static PipelineYamlJpaQuery jpa() {
        return new PipelineYamlJpaQuery(
            "com.example.CustomerRiskEntity",
            Map.of("customerId", "input.customerId"),
            Map.of("customerId", "customerId", "riskBand", "riskBand"),
            "single");
    }
}
