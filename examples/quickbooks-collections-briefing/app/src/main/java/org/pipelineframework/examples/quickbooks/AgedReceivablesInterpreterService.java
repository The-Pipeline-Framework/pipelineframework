package org.pipelineframework.examples.quickbooks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.JsonPayload;
import org.pipelineframework.examples.quickbooks.domain.CollectionAccount;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.service.ReactiveService;

/** Deterministic, application-owned interpretation of QuickBooks' unstructured MCP result. */
@ApplicationScoped
public class AgedReceivablesInterpreterService implements ReactiveService<JsonPayload, CollectionsBriefing> {
    static final String MCP_RESULT_SCHEMA = "urn:tpf:mcp:call-tool-result:v1";

    private final ObjectMapper json;

    @Inject
    public AgedReceivablesInterpreterService(ObjectMapper json) {
        this.json = json;
    }

    @Override
    public Uni<CollectionsBriefing> process(JsonPayload payload) {
        return Uni.createFrom().item(() -> interpret(payload));
    }

    CollectionsBriefing interpret(JsonPayload payload) {
        if (!"application/json".equals(payload.contentType()) || !MCP_RESULT_SCHEMA.equals(payload.schemaHint())) {
            throw new IllegalArgumentException("Expected an MCP CallToolResult JSON payload");
        }
        try {
            JsonNode envelope = json.readTree(payload.bodyJson());
            if (envelope.path("isError").asBoolean(false)) {
                throw new IllegalArgumentException("QuickBooks returned an MCP error result");
            }
            JsonNode report = reportFrom(envelope.path("content"));
            return briefingFrom(report);
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

    private CollectionsBriefing briefingFrom(JsonNode report) {
        Map<String, Integer> columns = columnIndexes(report.path("Columns").path("Column"));
        int customerIndex = columns.getOrDefault("customer", 0);
        List<CollectionAccount> accounts = new ArrayList<>();
        for (JsonNode row : report.path("Rows").path("Row")) {
            JsonNode cells = row.path("ColData");
            String customer = text(cells, customerIndex);
            if (customer.isBlank() || "total".equalsIgnoreCase(customer)) {
                continue;
            }
            accounts.add(new CollectionAccount(
                customer,
                amount(cells, requiredIndex(columns, "current")),
                amount(cells, requiredIndex(columns, "1 - 30")),
                amount(cells, requiredIndex(columns, "31 - 60")),
                amount(cells, requiredIndex(columns, "61 - 90")),
                amount(cells, requiredIndex(columns, "91 and over")),
                amount(cells, requiredIndex(columns, "total"))));
        }
        accounts.sort((left, right) -> overdue(right).compareTo(overdue(left)));
        BigDecimal total = accounts.stream().map(CollectionAccount::total)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal overdue = accounts.stream().map(AgedReceivablesInterpreterService::overdue)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        String currency = report.path("Header").path("Currency").asText("unknown");
        String reportDate = report.path("Header").path("EndPeriod").asText("unknown");
        String headline = accounts.isEmpty()
            ? "No open receivables were returned for " + reportDate + "."
            : "%d customers owe %s %s; %s %s is overdue. First call: %s (%s %s overdue).".formatted(
                accounts.size(), currency, money(total), currency, money(overdue), accounts.getFirst().customer(),
                currency, money(overdue(accounts.getFirst())));
        return new CollectionsBriefing(reportDate, currency, total, overdue, headline, accounts);
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

    private static BigDecimal overdue(CollectionAccount account) {
        return account.days1To30().add(account.days31To60()).add(account.days61To90()).add(account.over90());
    }

    private static String money(BigDecimal amount) {
        return amount.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }
}
