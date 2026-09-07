package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.modelcontextprotocol.client.McpAsyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import org.pipelineframework.connector.mcp.McpClientConnection;
import org.pipelineframework.connector.JsonPayload;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequest;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParams;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParamsAgingMethodValue;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.type.CanonicalFieldValue;

@QuarkusTest
class QuickBooksCollectionsBriefingIT {
    @InjectMock
    QuickBooksMcpConnectionResolver connectionResolver;

    @Inject
    @Any
    Instance<StepOneToOne<QuickBooksAgedReceivablesRequest, JsonPayload>> querySteps;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    @Inject
    AgedReceivablesInterpreterService interpreter;

    private McpAsyncClient client;
    private String executionId;

    @BeforeEach
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void configureHostConnection() {
        client = org.mockito.Mockito.mock(McpAsyncClient.class);
        when(client.isInitialized()).thenReturn(true);
        McpClientConnection connection = new McpClientConnection(client);
        when(connectionResolver.resolve(any())).thenReturn((CompletableFuture) CompletableFuture.completedFuture(
            connection));
        when(connectionResolver.tenantId()).thenReturn("sandbox-company");
        executionId = "quickbooks-collections-" + UUID.randomUUID();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("sandbox-company", executionId, 0));
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void importedMcpQueryProducesTypedBriefingAndReplaysWithoutASecondCall() {
        when(client.callTool(any())).thenReturn(Mono.just(McpSchema.CallToolResult.builder()
            .addTextContent("Aged Receivables")
            .addTextContent(reportJson())
            .isError(false)
            .build()));
        var request = new QuickBooksAgedReceivablesRequest(new QuickBooksAgedReceivablesRequestParams(
            CanonicalFieldValue.of(new QuickBooksAgedReceivablesRequestParamsAgingMethodValue("Report_Date")),
            CanonicalFieldValue.absent(), CanonicalFieldValue.absent(), CanonicalFieldValue.absent()));

        CollectionsBriefing first = invoke(request);
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("sandbox-company", executionId, 0));
        CollectionsBriefing replay = invoke(request);

        assertEquals(first, replay);
        assertEquals("Cafe One", first.accounts().getFirst().customer());
        assertTrue(first.headline().contains("First call: Cafe One"), first.headline());
        ArgumentCaptor<McpSchema.CallToolRequest> dispatched = ArgumentCaptor.forClass(McpSchema.CallToolRequest.class);
        verify(client, times(1)).callTool(dispatched.capture());
        assertEquals(Map.of("params", Map.of("aging_method", "Report_Date")), dispatched.getValue().arguments());
        verify(client, times(0)).listTools();
        verify(client, times(0)).close();
    }

    @Test
    void releaseMetadataContainsTheImportedOperationButNoHostRuntimeState() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String bindings = metadata("connector-bindings.json");

        assertTrue(contract.contains("QuickBooksAgedReceivablesRequest"), contract);
        assertTrue(bindings.contains("quickbooks.receivables.aged"), bindings);
        assertTrue(bindings.contains("\"name\": \"quickbooks\""), bindings);
        String publicReleaseData = contract + bindings;
        assertFalse(publicReleaseData.contains("QUICKBOOKS_CLIENT_SECRET"), publicReleaseData);
        assertFalse(publicReleaseData.contains("QUICKBOOKS_REFRESH_TOKEN"), publicReleaseData);
        assertFalse(publicReleaseData.contains("dist/index.js"), publicReleaseData);
        assertFalse(publicReleaseData.contains("get_aged_receivables"), publicReleaseData);
    }

    private CollectionsBriefing invoke(QuickBooksAgedReceivablesRequest request) {
        StepOneToOne<QuickBooksAgedReceivablesRequest, JsonPayload> query = querySteps.stream()
            .findFirst().orElseThrow(() -> new AssertionError("generated QuickBooks Query step bean not found"));
        JsonPayload payload = invocationRuntime.invokeStepUni(null, null, () -> query.applyOneToOne(request))
            .await().indefinitely();
        return interpreter.process(payload).await().indefinitely();
    }

    private static String reportJson() {
        return """
            {"Header":{"EndPeriod":"2026-09-07","Currency":"USD"},
             "Columns":{"Column":[{"ColTitle":""},{"ColTitle":"Current"},{"ColTitle":"1 - 30"},
               {"ColTitle":"31 - 60"},{"ColTitle":"61 - 90"},{"ColTitle":"91 and over"},{"ColTitle":"Total"}]},
             "Rows":{"Row":[{"ColData":[{"value":"Cafe One"},{"value":"10"},{"value":"20"},
               {"value":"30"},{"value":"40"},{"value":"50"},{"value":"150"}]}]}}
            """;
    }

    private static String metadata(String name) throws Exception {
        return Files.readString(Path.of("target/classes/META-INF/pipeline", name));
    }
}
