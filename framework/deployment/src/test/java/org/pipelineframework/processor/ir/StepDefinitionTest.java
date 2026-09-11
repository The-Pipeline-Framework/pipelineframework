package org.pipelineframework.processor.ir;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepDefinitionTest {
    private static final ClassName INPUT = ClassName.get("com.example", "Input");
    private static final ClassName OUTPUT = ClassName.get("com.example", "Output");
    private static final ClassName SERVICE = ClassName.get("com.example", "Service");

    @Test
    void authoredOperationMayDeclareTypedDeferredCompletion() {
        DeferredCompletionDefinition completion = new DeferredCompletionDefinition(
            "<PendingApproval>", Optional.of(ClassName.get("com.example", "PendingApproval")),
            "PT30M", List.of("orderId"), "interactionId", "interaction-api", Map.of(), Optional.empty());

        StepDefinition step = authored().withDeferredCompletion(completion);

        assertEquals(StepKind.INTERNAL, step.kind());
        assertEquals(completion, step.deferredCompletion().orElseThrow());
        assertEquals(List.of("orderId"), step.deferredCompletion().orElseThrow().idempotencyKeyFields());
    }

    @Test
    void deferredCompletionIsImmutable() {
        java.util.ArrayList<String> fields = new java.util.ArrayList<>(List.of("orderId"));
        java.util.LinkedHashMap<String, Object> config = new java.util.LinkedHashMap<>(Map.of("channel", "approval"));
        DeferredCompletionDefinition completion = new DeferredCompletionDefinition(
            "<PendingApproval>", Optional.empty(), "PT30M", fields, "interactionId",
            "interaction-api", config, Optional.empty());
        fields.add("tenantId");
        config.put("other", true);

        assertEquals(List.of("orderId"), completion.idempotencyKeyFields());
        assertEquals(Map.of("channel", "approval"), completion.transportConfig());
        assertThrows(UnsupportedOperationException.class,
            () -> completion.transportConfig().put("x", "y"));
    }

    @Test
    void onlyInternalAuthoredOperationsMayBeDecorated() {
        StepDefinition query = new StepDefinition(
            "Query", StepKind.QUERY, null, Optional.empty(), null,
            null, null, null, Map.of(), "query", Map.of(), List.of(),
            null, null, null, MapperFallbackMode.NONE, INPUT, OUTPUT,
            StreamingShape.UNARY_UNARY, false, List.of(), false,
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        DeferredCompletionDefinition completion = new DeferredCompletionDefinition(
            "<PendingApproval>", Optional.empty(), "PT30M", List.of(), "interactionId",
            "interaction-api", Map.of(), Optional.empty());

        assertThrows(IllegalArgumentException.class, () -> query.withDeferredCompletion(completion));
    }

    @Test
    void rejectsBlankNameAndMissingImplementation() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            " ", StepKind.INTERNAL, SERVICE, null, null, null,
            MapperFallbackMode.NONE, INPUT, OUTPUT, StreamingShape.UNARY_UNARY));
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Step", StepKind.INTERNAL, null, null, null, null,
            MapperFallbackMode.NONE, INPUT, OUTPUT, StreamingShape.UNARY_UNARY));
    }

    @Test
    void connectorSelectionCopyRetainsDeferredCompletion() {
        DeferredCompletionDefinition completion = new DeferredCompletionDefinition(
            "<PendingApproval>", Optional.empty(), "PT30M", List.of(), "interactionId",
            "interaction-api", Map.of(), Optional.empty());
        ConnectorOperationSelection selection = ConnectorOperationSelection.query(
            "Lookup",
            ConnectorBindingName.of("work"),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("example.provider"), "lookup", ConnectorOperationKind.QUERY, 1),
            1,
            Map.of(),
            new ConnectorOperationSelection.QuerySelection(
                QueryOperationCardinality.ONE_TO_ONE,
                QueryCapabilities.conservative(),
                Optional.empty(),
                Map.of(),
                List.of()));

        StepDefinition step = authored()
            .withDeferredCompletion(completion)
            .withConnectorOperationSelection(selection);

        assertEquals(completion, step.deferredCompletion().orElseThrow());
        assertEquals(selection, step.connectorOperationSelection().orElseThrow());
    }

    private StepDefinition authored() {
        return new StepDefinition(
            "Create pending approval", StepKind.INTERNAL, SERVICE, null, null, null,
            MapperFallbackMode.NONE, INPUT, OUTPUT, StreamingShape.UNARY_UNARY);
    }
}
