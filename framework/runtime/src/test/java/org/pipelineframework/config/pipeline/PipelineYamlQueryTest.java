package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
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
        assertEquals("eq", jpa.where().get("customerId").operator());
        assertEquals(List.of("input.customerId"), jpa.where().get("customerId").values());
        assertThrows(UnsupportedOperationException.class,
            () -> jpa.where().put("other", PipelineYamlJpaPredicate.equalTo("input.other")));
    }

    @Test
    void validatesJpaPredicateOrderAndLimitConfig() {
        PipelineYamlJpaQuery jpa = new PipelineYamlJpaQuery(
            "com.example.CustomerRiskEntity",
            Map.of(
                "score", new PipelineYamlJpaPredicate("gte", List.of("input.minimumScore")),
                "deletedAt", new PipelineYamlJpaPredicate("isNull", List.of("true"))),
            Map.of("accountStatus", "account.status"),
            Map.of("updatedAt", "DESC"),
            1,
            "single");

        assertEquals("gte", jpa.where().get("score").operator());
        assertEquals(List.of("input.minimumScore"), jpa.where().get("score").values());
        assertEquals(List.of(Boolean.TRUE), jpa.where().get("deletedAt").values());
        assertEquals("desc", jpa.orderBy().get("updatedAt"));
        assertEquals(1, jpa.limit());
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

    @Test
    void rejectsInvalidJpaPredicateShapes() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlJpaPredicate("contains", List.of("HIGH")));
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlJpaPredicate("between", List.of("input.low")));
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlJpaPredicate("isNull", List.of("yes")));
    }

    @Test
    void rejectsLimitWithoutOrderBy() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineYamlJpaQuery(
                "com.example.CustomerRiskEntity",
                Map.of("customerId", PipelineYamlJpaPredicate.equalTo("input.customerId")),
                Map.of(),
                Map.of(),
                1,
                "single"));
    }

    private static PipelineYamlJpaQuery jpa() {
        return new PipelineYamlJpaQuery(
            "com.example.CustomerRiskEntity",
            Map.of("customerId", "input.customerId"),
            Map.of("customerId", "customerId", "riskBand", "riskBand"),
            "single");
    }
}
