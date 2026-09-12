package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class PipelineYamlStepQueryFieldsTest {

    @Test
    void queryStepConstructorSetsQueryIdAndCapture() {
        PipelineYamlQueryCapture capture = new PipelineYamlQueryCapture(List.of("customerId"));
        PipelineYamlStep step = new PipelineYamlStep(
            "Load Customer Risk",
            "query",
            "ONE_TO_ONE",
            "com.example.CustomerRiskLookup",
            null,
            "com.example.CustomerRiskSnapshot",
            null,
            null,
            List.of(),
            null,
            "customer-risk-by-id",
            capture);

        assertEquals("Load Customer Risk", step.name());
        assertEquals("query", step.kind());
        assertEquals("customer-risk-by-id", step.queryId());
        assertEquals(List.of("customerId"), step.queryCapture().keyFields());
    }

    @Test
    void defaultsQueryCaptureToEmptyWhenNull() {
        PipelineYamlStep step = new PipelineYamlStep(
            "Load Risk",
            "query",
            "ONE_TO_ONE",
            "com.example.RiskLookup",
            null,
            "com.example.RiskSnapshot",
            null,
            null,
            List.of(),
            null,
            "risk-query",
            null);

        assertNotNull(step.queryCapture());
        assertTrue(step.queryCapture().keyFields().isEmpty());
    }

    @Test
    void nonQueryStepHasNullQueryId() {
        PipelineYamlStep step = new PipelineYamlStep(
            "Process Order",
            "com.example.OrderType",
            "com.example.OrderResult");

        assertNull(step.queryId());
    }

    @Test
    void nonQueryStepHasEmptyQueryCapture() {
        PipelineYamlStep step = new PipelineYamlStep(
            "Process Order",
            "com.example.OrderType",
            "com.example.OrderResult");

        assertNotNull(step.queryCapture());
        assertTrue(step.queryCapture().keyFields().isEmpty());
    }

    @Test
    void defaultsKindToInternalWhenNull() {
        PipelineYamlStep step = new PipelineYamlStep(
            "My Step",
            null,
            "ONE_TO_ONE",
            "com.example.Input",
            null,
            "com.example.Output",
            null,
            null,
            null,
            null,
            null,
            null);

        assertEquals("internal", step.kind());
    }

    @Test
    void defaultsCardinalityToOneToOneWhenNull() {
        PipelineYamlStep step = new PipelineYamlStep(
            "My Step",
            "internal",
            null,
            "com.example.Input",
            null,
            "com.example.Output",
            null,
            null,
            null,
            null,
            null,
            null);

        assertEquals("ONE_TO_ONE", step.cardinality());
    }

    @Test
    void defaultsIdempotencyKeyFieldsToEmptyListWhenNull() {
        PipelineYamlStep step = new PipelineYamlStep(
            "My Step",
            "query",
            "ONE_TO_ONE",
            "com.example.Input",
            null,
            "com.example.Output",
            null,
            null,
            null,
            null,
            "some-query",
            null);

        assertNotNull(step.idempotencyKeyFields());
        assertTrue(step.idempotencyKeyFields().isEmpty());
    }

    @Test
    void twoArgConvenienceConstructorSetsNullQueryFields() {
        PipelineYamlStep step = new PipelineYamlStep(
            "Process Entity",
            "com.example.EntityInput",
            "com.example.EntityOutput");

        assertNull(step.queryId());
        assertNotNull(step.queryCapture());
        assertTrue(step.queryCapture().keyFields().isEmpty());
    }
}