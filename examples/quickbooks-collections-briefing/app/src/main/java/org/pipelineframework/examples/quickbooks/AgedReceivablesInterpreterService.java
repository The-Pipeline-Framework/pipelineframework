package org.pipelineframework.examples.quickbooks;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.JsonPayload;
import org.pipelineframework.examples.quickbooks.domain.CollectionAccount;
import org.pipelineframework.service.ReactiveStreamingService;

/** Deterministically projects one unstructured QuickBooks report into typed customer rows. */
@ApplicationScoped
public class AgedReceivablesInterpreterService implements ReactiveStreamingService<JsonPayload, CollectionAccount> {
    static final String MCP_RESULT_SCHEMA = "urn:tpf:mcp:call-tool-result:v1";

    private final ObjectMapper json;

    @Inject
    public AgedReceivablesInterpreterService(ObjectMapper json) {
        this.json = json;
    }

    @Override
    public Multi<CollectionAccount> process(JsonPayload payload) {
        return Multi.createFrom().iterable(interpret(payload));
    }

    List<CollectionAccount> interpret(JsonPayload payload) {
        if (!"application/json".equals(payload.contentType()) || !MCP_RESULT_SCHEMA.equals(payload.schemaHint())) {
            throw new IllegalArgumentException("Expected an MCP CallToolResult JSON payload");
        }
        try {
            JsonNode envelope = json.readTree(payload.bodyJson());
            if (envelope.path("isError").asBoolean(false)) {
                throw new IllegalArgumentException("QuickBooks returned an MCP error result");
            }
            JsonNode report = reportFrom(envelope.path("content"));
            return accountsFrom(report);
        } catch (IllegalArgumentException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalArgumentException("QuickBooks aged-receivables result is not readable", failure);
        }
    }

    private JsonNode reportFrom(JsonNode content) {
        if (!content.isArray()) {
            throw new IllegalArgumentException("MCP result does not contain content blocks");
        }
        for (JsonNode block : content) {
            if (!"text".equals(block.path("type").asText()) || !block.path("text").isTextual()) {
                continue;
            }
            try {
                JsonNode candidate = json.readTree(block.path("text").asText());
                if (candidate.isObject() && candidate.has("Header")
                    && candidate.has("Columns") && candidate.has("Rows")) {
                    return candidate;
                }
            } catch (Exception ignored) {
                // A CallToolResult can contain human-readable text alongside the JSON report.
            }
        }
        throw new IllegalArgumentException("MCP result does not contain a QuickBooks report");
    }

    private List<CollectionAccount> accountsFrom(JsonNode report) {
        Map<String, Integer> columns = columnIndexes(report.path("Columns").path("Column"));
        int customerIndex = columns.getOrDefault("customer", 0);
        String currency = report.path("Header").path("Currency").asText("unknown");
        String reportDate = report.path("Header").path("EndPeriod").asText("unknown");
        List<CollectionAccount> accounts = new ArrayList<>();
        for (JsonNode row : report.path("Rows").path("Row")) {
            JsonNode cells = row.path("ColData");
            String customer = text(cells, customerIndex);
            if (customer.isBlank() || "total".equalsIgnoreCase(customer)) {
                continue;
            }
            accounts.add(new CollectionAccount(
                reportDate,
                currency,
                true,
                cells.path(customerIndex).path("id").asText(""),
                customer,
                amount(cells, requiredIndex(columns, "current")),
                amount(cells, requiredIndex(columns, "1 - 30")),
                amount(cells, requiredIndex(columns, "31 - 60")),
                amount(cells, requiredIndex(columns, "61 - 90")),
                amount(cells, requiredIndex(columns, "91 and over")),
                amount(cells, requiredIndex(columns, "total"))));
        }
        if (accounts.isEmpty()) {
            accounts.add(new CollectionAccount(reportDate, currency, false, "", "", BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO));
        }
        return List.copyOf(accounts);
    }

    private static Map<String, Integer> columnIndexes(JsonNode columns) {
        Map<String, Integer> indexes = new HashMap<>();
        for (int index = 0; index < columns.size(); index++) {
            indexes.put(normalize(columns.get(index).path("ColTitle").asText()), index);
        }
        return Map.copyOf(indexes);
    }

    private static int requiredIndex(Map<String, Integer> columns, String name) {
        Integer index = columns.get(normalize(name));
        if (index == null) {
            throw new IllegalArgumentException("QuickBooks report is missing the '" + name + "' column");
        }
        return index;
    }

    private static String normalize(String value) {
        return value.strip().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    private static String text(JsonNode cells, int index) {
        return cells.path(index).path("value").asText("").strip();
    }

    private static BigDecimal amount(JsonNode cells, int index) {
        String value = text(cells, index).replace(",", "");
        return value.isBlank() ? BigDecimal.ZERO : new BigDecimal(value);
    }

}
