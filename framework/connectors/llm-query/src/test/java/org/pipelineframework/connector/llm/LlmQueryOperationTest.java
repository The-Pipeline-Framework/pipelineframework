package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorConfigurationBinder;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;

class LlmQueryOperationTest {
    private static final LlmTurnConfiguration CONFIGURATION = new LlmTurnConfiguration(
        "Choose one alternative.",
        Map.of("charge", new LlmCallableConfiguration(
            "payments", "charge.create", "command", 1, "ToolArguments")));

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
    void providerFailuresRemainExceptional() {
        LlmQueryOperation operation = operation(request -> CompletableFuture.failedStage(
            new IllegalStateException("provider unavailable")));

        assertThrows(java.util.concurrent.CompletionException.class, () -> operation.query(invocation()).toCompletableFuture().join());
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

        assertEquals("payments", bound.callables().get("charge").using());
        assertEquals("command", bound.callables().get("charge").kind());
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

    private static QueryOutcome<Object> query(LlmDecisionClient client) {
        return operation(client).query(invocation()).toCompletableFuture().join();
    }

    private static LlmQueryOperation operation(LlmDecisionClient client) {
        return new LlmQueryOperation(() -> Optional.of(client));
    }

    private static QueryInvocation<Object, LlmTurnConfiguration, Object> invocation() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Object> output = (Class) Decision.class;
        return new QueryInvocation<>(Map.of("invoiceId", "7"), CONFIGURATION, output, ConnectorExecutionContext.empty());
    }
}
