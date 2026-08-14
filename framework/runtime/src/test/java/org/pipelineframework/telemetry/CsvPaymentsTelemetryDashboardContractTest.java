package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CsvPaymentsTelemetryDashboardContractTest {
    private static final Path GRAFANA = Path.of("..", "..", "examples", "csv-payments", "orchestrator-svc",
        "src", "main", "resources", "META-INF", "grafana", "grafana-dashboard-csv-payments.json");
    private static final Path TEMPO = Path.of("..", "..", "examples", "csv-payments", "orchestrator-svc",
        "src", "main", "resources", "META-INF", "grafana", "grafana-dashboard-csv-payments-tempo.json");

    @Test
    void dashboardQueriesProveCanonicalCsvJourneyObligations() throws Exception {
        JsonNode metrics = new ObjectMapper().readTree(Files.readString(GRAFANA));
        JsonNode tempo = new ObjectMapper().readTree(Files.readString(TEMPO));
        List<String> metricQueries = queryValues(metrics, "expr");
        List<String> traceQueries = queryValues(tempo, "query");

        for (ObservabilityObligations.Obligation obligation : ObservabilityObligations.CSV_PAYMENTS_JOURNEY) {
            for (String metric : obligation.requiredMetricNames()) {
                assertTrue(metricQueries.stream().anyMatch(query -> query.contains(metric)),
                    () -> obligation.transition() + " lacks a metrics query for " + metric);
            }
            for (String span : obligation.requiredTraceSpanNames()) {
                assertTrue(traceQueries.stream().anyMatch(query -> query.contains(span)),
                    () -> obligation.transition() + " lacks a Tempo query for " + span);
            }
        }
    }

    @Test
    void tempoProofDeclaresProfileAndRootlessAwaitDiagnostic() throws Exception {
        JsonNode tempo = new ObjectMapper().readTree(Files.readString(TEMPO));
        List<String> traceQueries = queryValues(tempo, "query");
        assertTrue(Files.readString(TEMPO).contains("telemetry-capable CSV Payments developer profile"));
        assertTrue(Files.readString(TEMPO).contains("self-host container profile, which intentionally disables telemetry"));
        assertTrue(traceQueries.stream().anyMatch(query -> query.contains("tpf.await.origin.linked") && query.contains("false")),
            "Tempo dashboard must expose rootless/unlinked Await completion spans");
        assertTrue(traceQueries.stream().anyMatch(query -> query.contains("tpf.await.origin.linked") && query.contains("nil")),
            "Tempo dashboard must expose Await completion spans missing origin-link metadata");
    }

    @Test
    void operatorDashboardPreservesCurrentOperationalPanelsWithoutHighCardinalityDimensions() throws Exception {
        JsonNode metrics = new ObjectMapper().readTree(Files.readString(GRAFANA));
        List<String> metricQueries = queryValues(metrics, "expr");
        Set<String> panelTitles = Set.copyOf(queryValues(metrics, "title"));

        for (ObservabilityObligations.OperatorPanel panel : ObservabilityObligations.CSV_PAYMENTS_OPERATOR_PANELS) {
            assertTrue(panelTitles.contains(panel.title()), "Missing restored operator panel: " + panel.title());
            for (String metric : panel.requiredMetricNames()) {
                assertTrue(metricQueries.stream().anyMatch(query -> query.contains(metric)),
                    () -> panel.title() + " lacks a query for " + metric);
            }
        }

        assertTrue(metricQueries.stream().anyMatch(query -> query.contains("tpfProof")) == false,
            "The dashboard proof marker is target metadata, not PromQL.");
        for (String forbidden : List.of("execution_id", "interaction_id", "correlation_id", "request_id", "item_index")) {
            assertTrue(metricQueries.stream().noneMatch(query -> query.contains(forbidden)),
                () -> "Metric dashboard must not use high-cardinality dimension " + forbidden);
        }
    }

    private static List<String> queryValues(JsonNode node, String field) {
        List<String> values = new ArrayList<>();
        collect(node, field, values);
        return List.copyOf(values);
    }

    private static void collect(JsonNode node, String field, List<String> values) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                if (field.equals(entry.getKey()) && entry.getValue().isTextual()) {
                    values.add(entry.getValue().asText());
                }
                collect(entry.getValue(), field, values);
            });
        } else if (node.isArray()) {
            node.forEach(value -> collect(value, field, values));
        }
    }
}
