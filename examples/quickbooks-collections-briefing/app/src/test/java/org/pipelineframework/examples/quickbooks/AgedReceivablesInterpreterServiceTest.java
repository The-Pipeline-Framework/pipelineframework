package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.pipelineframework.connector.JsonPayload;

class AgedReceivablesInterpreterServiceTest {
    private final ObjectMapper json = new ObjectMapper();
    private final AgedReceivablesInterpreterService interpreter = new AgedReceivablesInterpreterService(json);

    @Test
    void turnsQuickBooksReportTextIntoPrioritizedTypedAccounts() throws Exception {
        String report = """
            {
              "Header":{"EndPeriod":"2026-09-07","Currency":"USD"},
              "Columns":{"Column":[
                {"ColTitle":"Customer"},{"ColTitle":"Current"},{"ColTitle":"1 - 30"},
                {"ColTitle":"31 - 60"},{"ColTitle":"61 - 90"},{"ColTitle":"91 and over"},
                {"ColTitle":"Total"}
              ]},
              "Rows":{"Row":[
                {"ColData":[{"value":"Blue Diner"},{"value":"100.00"},{"value":"40.00"},
                  {"value":"20.00"},{"value":""},{"value":"10.00"},{"value":"170.00"}]},
                {"ColData":[{"value":"Green Supply"},{"value":"25.00"},{"value":""},
                  {"value":"200.00"},{"value":"30.00"},{"value":""},{"value":"255.00"}]}
              ]}
            }
            """;
        String body = json.writeValueAsString(json.createObjectNode()
            .put("isError", false)
            .set("content", json.createArrayNode()
                .add(json.createObjectNode().put("type", "text").put("text", "Aged Receivables report"))
                .add(json.createObjectNode().put("type", "text").put("text", report))));

        var briefing = interpreter.interpret(new JsonPayload(
            "application/json", AgedReceivablesInterpreterService.MCP_RESULT_SCHEMA, body));

        assertEquals("2026-09-07", briefing.reportDate());
        assertEquals("USD", briefing.currency());
        assertEquals(new BigDecimal("425.00"), briefing.totalOutstanding());
        assertEquals(new BigDecimal("300.00"), briefing.overdueOutstanding());
        assertEquals("Green Supply", briefing.accounts().getFirst().customer());
        assertEquals(new BigDecimal("230.00"), briefing.accounts().getFirst().days31To60()
            .add(briefing.accounts().getFirst().days61To90()));
        assertEquals("2 customers owe USD 425.00; USD 300.00 is overdue. "
            + "First call: Green Supply (USD 230.00 overdue).", briefing.headline());
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
}
