package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.JsonPayload;

class AgedReceivablesInterpreterServiceTest {
    private final ObjectMapper json = new ObjectMapper();
    private final AgedReceivablesInterpreterService interpreter = new AgedReceivablesInterpreterService(json);

    @Test
    void turnsSandboxFixtureIntoTypedCustomerRows() throws Exception {
        var accounts = interpreter.interpret(payloadForFixture(
            "/fixtures/qbo-sandbox-aged-receivables-2026-09-08.json"));

        assertEquals(4, accounts.size());
        assertEquals("2026-09-08", accounts.getFirst().reportDate());
        assertEquals("GBP", accounts.getFirst().currency());
        assertEquals("67", accounts.getFirst().customerId());
        assertEquals("Abercrombie International Group", accounts.getFirst().customer());
        assertEquals(new BigDecimal("30620.82"), accounts.getFirst().over90());
        assertEquals(new BigDecimal("480.00"), accounts.getLast().total());
    }

    @Test
    void retainsReportIdentityWhenNoCustomerRowsExist() throws Exception {
        var reportItems = interpreter.interpret(payloadForFixture(
            "/fixtures/qbo-empty-aged-receivables.json"));

        assertEquals(1, reportItems.size());
        assertFalse(reportItems.getFirst().accountPresent());
        assertEquals("2026-09-08", reportItems.getFirst().reportDate());
        assertEquals("GBP", reportItems.getFirst().currency());

        var briefing = CollectionsBriefingService.briefing(List.of(
            CollectionPolicyService.assess(reportItems.getFirst())));
        assertEquals(0, briefing.actions().size());
        assertEquals(BigDecimal.ZERO, briefing.totalOutstanding());
        assertEquals(BigDecimal.ZERO, briefing.overdueOutstanding());
        assertEquals("No open receivables were returned for 2026-09-08.", briefing.headline());
    }

    @Test
    void failsClearlyWhenNoMachineReadableReportExists() throws Exception {
        String body = json.writeValueAsString(json.createObjectNode().put("isError", false)
            .set("content", json.createArrayNode()
                .add(json.createObjectNode().put("type", "text").put("text", "No report today"))));

        var failure = assertThrows(IllegalArgumentException.class, () -> interpreter.interpret(new JsonPayload(
            "application/json", AgedReceivablesInterpreterService.MCP_RESULT_SCHEMA, body)));

        assertEquals("MCP result does not contain a QuickBooks report", failure.getMessage());
    }

    private JsonPayload payloadForFixture(String fixture) throws Exception {
        String report;
        try (var input = Objects.requireNonNull(getClass().getResourceAsStream(fixture))) {
            report = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        String body = json.writeValueAsString(json.createObjectNode()
            .put("isError", false)
            .set("content", json.createArrayNode()
                .add(json.createObjectNode().put("type", "text").put("text", "Aged Receivables report"))
                .add(json.createObjectNode().put("type", "text").put("text", report))));
        return new JsonPayload("application/json", AgedReceivablesInterpreterService.MCP_RESULT_SCHEMA, body);
    }
}
