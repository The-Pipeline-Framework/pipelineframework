package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.modelcontextprotocol.client.McpAsyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.connector.mcp.McpClientConnection;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequest;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParams;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParamsAgingMethodValue;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.type.CanonicalFieldValue;

@QuarkusTest
@TestProfile(QuickBooksReplayTestProfile.class)
class QuickBooksCollectionsReplayIT {
    @InjectMock
    QuickBooksMcpConnectionResolver connectionResolver;

    @Inject
    PipelineExecutionService executionService;

    private McpAsyncClient client;

    @BeforeEach
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void configureScenario() throws IOException {
        try (var files = Files.list(QuickBooksReplayTestProfile.REPLAY_DIRECTORY)) {
            for (var file : files.toList()) {
                Files.delete(file);
            }
        }
        client = org.mockito.Mockito.mock(McpAsyncClient.class);
        when(client.isInitialized()).thenReturn(true);
        when(client.callTool(any())).thenReturn(Mono.just(McpSchema.CallToolResult.builder()
            .addTextContent("Aged Receivables")
            .addTextContent(QuickBooksCollectionsBriefingIT.reportJson())
            .isError(false)
            .build()));
        McpClientConnection connection = new McpClientConnection(client);
        when(connectionResolver.resolve(any())).thenReturn((CompletableFuture) CompletableFuture.completedFuture(
            connection));
        when(connectionResolver.tenantId()).thenReturn("sandbox-company");
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(
            "sandbox-company", "quickbooks-demo-" + UUID.randomUUID(), 0));
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void oneSandboxReportBecomesOneReplayableCollectionsBriefing() throws Exception {
        var request = new QuickBooksAgedReceivablesRequest(new QuickBooksAgedReceivablesRequestParams(
            CanonicalFieldValue.of(new QuickBooksAgedReceivablesRequestParamsAgingMethodValue("Report_Date")),
            CanonicalFieldValue.of(new BigDecimal("30")), CanonicalFieldValue.of(new BigDecimal("4")),
            CanonicalFieldValue.of("2026-09-08")));

        CollectionsBriefing briefing = executionService.<CollectionsBriefing>executePipelineUnary(
            Uni.createFrom().item(request)).await().indefinitely();

        assertEquals(4, briefing.actions().size());
        assertEquals("GBP", briefing.currency());
        assertEquals(new BigDecimal("33860.82"), briefing.totalOutstanding());
        assertEquals(new BigDecimal("33860.82"), briefing.overdueOutstanding());
        assertEquals(2, briefing.priorityAccounts());
        assertEquals("CRITICAL", briefing.actions().getFirst().priority());
        assertEquals("Abercrombie International Group", briefing.actions().getFirst().account().customer());
        verify(client, times(1)).callTool(any());

        var replayFiles = replayFiles();
        assertEquals(1, replayFiles.size(), "one external stimulus must create one root replay file");
        String replay = Files.readString(replayFiles.getFirst());
        assertTrue(replay.contains("\"step\" : \"ReadAgedReceivables\""), replay);
        assertTrue(replay.contains("\"step\" : \"ExtractReceivablesAccounts\""), replay);
        assertTrue(replay.contains("\"step\" : \"ApplyCollectionsPolicy\""), replay);
        assertTrue(replay.contains("\"step\" : \"PrepareCollectionsBriefing\""), replay);
    }

    private static java.util.List<java.nio.file.Path> replayFiles() throws IOException {
        try (var files = Files.list(QuickBooksReplayTestProfile.REPLAY_DIRECTORY)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".json")).toList();
        }
    }
}
