package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void declaresPipelineResultCachingSupport() {
        LlmQueryOperation operation = new LlmQueryOperation(Optional::<LlmDecisionClient>empty);

        assertEquals(org.pipelineframework.connector.QueryCacheability.CACHEABLE,
            operation.capabilities().cacheability());
    }
    private record InvoiceInput(PayloadReference payloadReference) { }
    private record InvoiceBatchInput(java.util.List<PayloadReference> payloadReferences) { }
    private record TextFirstInput(PayloadReference invoice, String evidence) { }
    private record ReviewInput(String invoiceId, String evidence) { }
    private record NestedReviewInput(ReviewInput invoice) { }

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
    void bindsDirectCompletionProjectionFromConnectorConfiguration() {
        LlmQueryOperation operation = operation(request -> CompletableFuture.completedFuture(
            new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}")));

        LlmTurnConfiguration bound = ConnectorConfigurationBinder.bind(
            operation.configurationSchema().orElseThrow(),
            new ConnectorConfigurationDocument(Map.of(
                "instructions", "Analyse once.",
                "completion", Map.of("field", "review", "invoiceId", "invoiceId"))),
            "test LLM Query");

        assertEquals("review", bound.directCompletion().orElseThrow().field());
        assertEquals(Map.of("invoiceId", "invoiceId"),
            bound.directCompletion().orElseThrow().carriedFields());
    }

    @Test
    void rejectsDottedDirectCompletionOutputComponentsButAllowsDottedInputPaths() {
        assertThrows(IllegalArgumentException.class,
            () -> new LlmDirectCompletionConfiguration("review.summary", Optional.of(Map.of())));
        assertThrows(IllegalArgumentException.class,
            () -> new LlmDirectCompletionConfiguration(
                "review", Optional.of(Map.of("invoice.id", "invoice.id"))));

        assertEquals(Map.of("invoiceId", "invoice.id"),
            new LlmDirectCompletionConfiguration(
                "review", Optional.of(Map.of("invoiceId", "invoice.id")))
                .carriedFields());
    }

    @Test
    void normalizesDirectCompletionCarryEntries() {
        assertEquals(Map.of("invoiceId", "invoice.invoiceId"),
            new LlmDirectCompletionConfiguration(
                "review", Optional.of(Map.of(" invoiceId ", " invoice.invoiceId ")))
                .carriedFields());
        assertThrows(IllegalArgumentException.class,
            () -> new LlmDirectCompletionConfiguration(
                "review",
                Optional.of(Map.of(
                    "invoiceId", "invoice.invoiceId",
                    " invoiceId ", "invoice.otherId"))));
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
        LlmDecisionClient client = new LlmDecisionClient() {
            @Override
            public boolean supportsNativeStructuredOutput(java.util.List<LlmToolDefinition> tools) {
                assertEquals(java.util.List.of("complete"),
                    tools.stream().map(LlmToolDefinition::alias).toList());
                return true;
            }

            @Override
            public java.util.concurrent.CompletionStage<LlmToolProposal> decide(LlmTurnRequest request) {
                observed.set(request);
                return CompletableFuture.completedFuture(
                    new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
            }
        };
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.", Map.of());
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
    void projectsDirectCompletionIntoOutputAndCarriesTrustedInputFields() {
        AtomicReference<LlmTurnRequest> observed = new AtomicReference<>();
        LlmDecisionClient client = new LlmDecisionClient() {
            @Override
            public boolean supportsNativeStructuredOutput(java.util.List<LlmToolDefinition> tools) {
                return true;
            }

            @Override
            public java.util.concurrent.CompletionStage<LlmToolProposal> decide(LlmTurnRequest request) {
                observed.set(request);
                return CompletableFuture.completedFuture(
                    new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
            }
        };
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.",
            Optional.of(Map.of()),
            Optional.empty(),
            Optional.of(Map.of("field", "review", "invoiceId", "invoiceId")));
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewEnvelope.class;

        QueryOutcome<Object> outcome = operation(client).query(new QueryInvocation<>(
            new ReviewInput("invoice-7", "untrusted evidence"),
            completion,
            output,
            ConnectorExecutionContext.empty())).toCompletableFuture().join();

        ReviewEnvelope envelope = assertInstanceOf(ReviewEnvelope.class,
            assertInstanceOf(QueryOutcome.Found.class, outcome).output());
        assertEquals("invoice-7", envelope.invoiceId());
        assertEquals("Acme", envelope.review().supplier());
        org.junit.jupiter.api.Assertions.assertTrue(
            observed.get().tools().getFirst().inputSchemaJson().contains("supplier"));
        org.junit.jupiter.api.Assertions.assertFalse(
            observed.get().tools().getFirst().inputSchemaJson().contains("invoiceId"));
    }

    @Test
    void nullIntermediateCarryValueIsAnInvalidModelDecision() {
        LlmDecisionClient client = new LlmDecisionClient() {
            @Override
            public boolean supportsNativeStructuredOutput(java.util.List<LlmToolDefinition> tools) {
                return true;
            }

            @Override
            public java.util.concurrent.CompletionStage<LlmToolProposal> decide(LlmTurnRequest request) {
                return CompletableFuture.completedFuture(
                    new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
            }
        };
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.",
            Optional.of(Map.of()),
            Optional.empty(),
            Optional.of(Map.of("field", "review", "invoiceId", "invoice.invoiceId")));
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewEnvelope.class;

        QueryOutcome<Object> outcome = operation(client).query(new QueryInvocation<>(
            new NestedReviewInput(null),
            completion,
            output,
            ConnectorExecutionContext.empty())).toCompletableFuture().join();

        QueryOutcome.TerminalFailure<?> failure = assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome);
        assertEquals("invalid-model-decision", failure.code());
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

    @Test
    void excludedCarriedPayloadIsNeitherPromptStateNorMedia() {
        AtomicReference<LlmTurnRequest> observed = new AtomicReference<>();
        AtomicInteger materializations = new AtomicInteger();
        PayloadReference reference = new PayloadReference(
            "test", "invoices", "invoice.pdf", "application/pdf", null, "sha256:test", 4,
            null, Map.of(), Optional.empty());
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse text once.", Optional.of(Map.of()), Optional.of(StructuredOutputSchemaMode.OPTIONAL), Optional.empty(),
            Optional.of(Map.of("invoice", "invoice")));
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewReady.class;

        QueryOutcome<Object> outcome = operation(request -> {
            observed.set(request);
            return CompletableFuture.completedFuture(
                new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
        }).query(new QueryInvocation<>(
            new TextFirstInput(reference, "searchable invoice text"),
            completion,
            output,
            ConnectorExecutionContext.empty(),
            Optional.of((candidate, maxBytes) -> {
                materializations.incrementAndGet();
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    candidate, new byte[]{1}, candidate.contentType(), null, candidate.checksum()));
            }))).toCompletableFuture().join();

        assertInstanceOf(QueryOutcome.Found.class, outcome);
        assertEquals(0, materializations.get());
        assertEquals(java.util.List.of(), observed.get().media());
        org.junit.jupiter.api.Assertions.assertFalse(observed.get().applicationStateJson().contains("invoice.pdf"));
        org.junit.jupiter.api.Assertions.assertTrue(
            observed.get().applicationStateJson().contains("searchable invoice text"));
    }

    @Test
    void combinedMaterializedPayloadsCannotExceedTheTotalBudget() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger materializations = new AtomicInteger();
        int payloadBytes = 11 * 1024 * 1024;
        PayloadReference first = new PayloadReference(
            "test", "invoices", "first.pdf", "application/pdf", null, "sha256:first", 1,
            null, Map.of(), Optional.empty());
        PayloadReference second = new PayloadReference(
            "test", "invoices", "second.pdf", "application/pdf", null, "sha256:second", 1,
            null, Map.of(), Optional.empty());
        LlmTurnConfiguration completion = new LlmTurnConfiguration(
            "Analyse once.", Map.of(), StructuredOutputSchemaMode.OPTIONAL);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) ReviewReady.class;
        QueryInvocation<Object, LlmTurnConfiguration, Object> invocation = new QueryInvocation<>(
            new InvoiceBatchInput(java.util.List.of(first, second)),
            completion,
            output,
            ConnectorExecutionContext.empty(),
            Optional.of((reference, maxBytes) -> {
                materializations.incrementAndGet();
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    reference, new byte[payloadBytes], reference.contentType(), null, reference.checksum()));
            }));

        QueryOutcome<Object> outcome = operation(request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(
                new LlmToolProposal("complete", "{\"supplier\":\"Acme\"}"));
        }).query(invocation).toCompletableFuture().join();

        assertEquals("llm-query-failed",
            assertInstanceOf(QueryOutcome.TerminalFailure.class, outcome).code());
        assertEquals(2, materializations.get());
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
