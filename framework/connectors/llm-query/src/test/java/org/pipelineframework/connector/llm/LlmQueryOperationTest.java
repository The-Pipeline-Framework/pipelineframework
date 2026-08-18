package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.type.CanonicalTypeCatalogue;

class LlmQueryOperationTest {
    private record InvoiceInput(PayloadReference payloadReference) { }

    private static final LlmTurnConfiguration CONFIGURATION = new LlmTurnConfiguration(
        "Choose one alternative.",
        Map.of("charge", new LlmCallableConfiguration(
            "payments", "charge.create", "command", 1, "ToolArguments")),
        StructuredOutputSchemaMode.OPTIONAL);

    @Test
    void performsOneInferenceAndConstructsTrustedBoundAgentCall() {
        AtomicInteger calls = new AtomicInteger();
        LlmDecisionClient client = request -> {
            calls.incrementAndGet();
            assertEquals(java.util.List.of("charge", "complete"),
                request.tools().stream().map(LlmToolDefinition::alias).toList());
            return CompletableFuture.completedFuture(new LlmToolProposal(
                "charge", "{\"note\":\"invoice 7\",\"amount\":42}"));
        };

        QueryOutcome<Object> outcome = query(client);

        Decision.Call call = assertInstanceOf(Decision.Call.class,
            assertInstanceOf(QueryOutcome.Found.class, outcome).output());
        assertEquals("payments", call.value().binding());
        assertEquals("charge.create", call.value().operation());
        assertEquals("{\"amount\":42,\"note\":\"invoice 7\"}", call.value().argumentsJson());
        assertEquals(1, calls.get());
    }

    @Test
    void invalidArgumentsAreAModelDecisionFailureRatherThanProviderFailure() {
        QueryOutcome<Object> outcome = query(request -> CompletableFuture.completedFuture(
            new LlmToolProposal("charge", "{\"amount\":42,\"unexpected\":true}")));

        QueryOutcome.TerminalFailure<?> failure = assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome);
        assertEquals("invalid-model-decision", failure.code());
    }

    @Test
    void providerFailureIsTerminalAfterExactlyOneInferenceAttempt() {
        AtomicInteger calls = new AtomicInteger();
        LlmQueryOperation operation = operation(request -> {
            calls.incrementAndGet();
            return CompletableFuture.failedStage(new IllegalStateException("provider unavailable"));
        });

        QueryOutcome<?> outcome = operation.query(invocation()).toCompletableFuture().join();

        assertEquals("llm-query-failed", assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome).code());
        assertEquals(1, calls.get());
    }

    @Test
    void requiredStructuredOutputFailsBeforeInferenceWhenAdapterCannotEnforceIt() {
        AtomicInteger calls = new AtomicInteger();
        LlmTurnConfiguration required = new LlmTurnConfiguration(
            CONFIGURATION.instructions(), CONFIGURATION.callableCatalogue());
        LlmDecisionClient unsupported = request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(new LlmToolProposal("complete", "{\"status\":\"ok\"}"));
        };

        QueryOutcome<?> outcome = operation(unsupported).query(invocation(required)).toCompletableFuture().join();

        assertEquals("structured-output-unavailable",
            assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome).code());
        assertEquals(0, calls.get());
    }

    @Test
    void bindsTheCompiledYamlCatalogueIntoTypedOperationConfiguration() {
        LlmQueryOperation operation = operation(request -> CompletableFuture.completedFuture(
            new LlmToolProposal("charge", "{\"amount\":42,\"note\":\"ok\"}")));

        LlmTurnConfiguration bound = ConnectorConfigurationBinder.bind(
            operation.configurationSchema().orElseThrow(),
            new ConnectorConfigurationDocument(Map.of(
                "instructions", "Decide once.",
                "callables", Map.of("charge", Map.of(
                    "using", "payments",
                    "operation", "charge.create",
                    "kind", "command",
                    "operationVersion", 1,
                    "input", "ToolArguments")))),
            "test LLM Query");

        assertEquals("payments", bound.callableCatalogue().get("charge").using());
        assertEquals("command", bound.callableCatalogue().get("charge").kind());
        assertEquals(StructuredOutputSchemaMode.REQUIRED, bound.structuredOutputMode());
    }

    @Test
    void buildsTheDecisionContractOncePerOutputAndConfigurationBinding() {
        AtomicInteger loads = new AtomicInteger();
        CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.load(Decision.class.getClassLoader());
        LlmQueryOperation operation = new LlmQueryOperation(
            () -> Optional.of(request -> CompletableFuture.completedFuture(
                new LlmToolProposal("charge", "{\"amount\":42,\"note\":\"ok\"}"))),
            ignored -> {
                loads.incrementAndGet();
                return catalogue;
            });

        operation.query(invocation()).toCompletableFuture().join();
        operation.query(invocation()).toCompletableFuture().join();

        assertEquals(1, loads.get());
    }

    @Test
    void completesDirectApplicationRecordAndMaterializesNestedPayloadOnce() {
        AtomicInteger materializations = new AtomicInteger();
        AtomicReference<LlmTurnRequest> observed = new AtomicReference<>();
        PayloadReference reference = new PayloadReference(
            "test", "invoices", "invoice.pdf", "application/pdf", null, "sha256:test", 4,
            null, Map.of(), Optional.empty());
        LlmDecisionClient client = request -> {
            observed.set(request);
            assertEquals(java.util.List.of("complete"),
                request.tools().stream().map(LlmToolDefinition::alias).toList());
            return CompletableFuture.completedFuture(
                new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
        };
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.", Map.of(), StructuredOutputSchemaMode.OPTIONAL);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewReady.class;
        QueryInvocation<Object, LlmTurnConfiguration, Object> invocation = new QueryInvocation<>(
            new InvoiceInput(reference),
            completion,
            output,
            ConnectorExecutionContext.empty(),
            Optional.of((candidate, maxBytes) -> {
                materializations.incrementAndGet();
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    candidate, new byte[]{1, 2, 3, 4}, "application/pdf", null, "sha256:test"));
            }));

        QueryOutcome<Object> outcome = operation(client).query(invocation).toCompletableFuture().join();

        ReviewReady review = assertInstanceOf(ReviewReady.class,
            assertInstanceOf(QueryOutcome.Found.class, outcome).output());
        assertEquals("Acme", review.supplier());
        assertEquals(1, materializations.get());
        assertEquals(1, observed.get().media().size());
    }

    @Test
    void missingPayloadMaterializerIsReportedAsTerminalFailure() {
        AtomicInteger calls = new AtomicInteger();
        PayloadReference reference = new PayloadReference(
            "test", "invoices", "invoice.pdf", "application/pdf", null, "sha256:test", 4,
            null, Map.of(), Optional.empty());
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.", Map.of(), StructuredOutputSchemaMode.OPTIONAL);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewReady.class;
        QueryInvocation<Object, LlmTurnConfiguration, Object> invocation = new QueryInvocation<>(
            new InvoiceInput(reference),
            completion,
            output,
            ConnectorExecutionContext.empty());

        QueryOutcome<Object> outcome = operation(request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
        }).query(invocation).toCompletableFuture().join();

        assertEquals("llm-query-failed",
            assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome).code());
        assertEquals(0, calls.get());
    }

    private static QueryOutcome<Object> query(LlmDecisionClient client) {
        return operation(client).query(invocation()).toCompletableFuture().join();
    }

    private static LlmQueryOperation operation(LlmDecisionClient client) {
        return new LlmQueryOperation(() -> Optional.of(client));
    }

    private static QueryInvocation<Object, LlmTurnConfiguration, Object> invocation() {
        return invocation(CONFIGURATION);
    }

    private static QueryInvocation<Object, LlmTurnConfiguration, Object> invocation(LlmTurnConfiguration configuration) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) Decision.class;
        return new QueryInvocation<>(Map.of("invoiceId", "7"), configuration, output, ConnectorExecutionContext.empty());
    }
}
