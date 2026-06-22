package org.pipelineframework.search.common.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;
import org.pipelineframework.search.common.dto.SearchIndexWriteResultDto;

class OpenSearchIndexDocumentCommandConnectorTest {
  private HttpServer server;

  @AfterEach
  void tearDown() {
    System.clearProperty("search.index.opensearch.endpoint");
    System.clearProperty("search.index.opensearch.timeout-seconds");
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void writesDocumentToConfiguredOpenSearchEndpoint() throws Exception {
    AtomicReference<String> path = new AtomicReference<>();
    AtomicReference<String> body = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      path.set(exchange.getRequestURI().getPath());
      body.set(new String(exchange.getRequestBody().readAllBytes()));
      exchange.sendResponseHeaders(200, 0);
      exchange.getResponseBody().close();
    });
    server.start();
    System.setProperty("search.index.opensearch.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
    System.setProperty("search.index.opensearch.timeout-seconds", "2");

    SearchIndexDocument document = document();
    SearchIndexWriteResult result = (SearchIndexWriteResult) new OpenSearchIndexDocumentCommandConnector()
        .execute(new CommandRequest<>(
            descriptor(),
            "cmd-1",
            document,
            new AwaitExecutionContext("tenant", "exec-1", 4),
            Map.of()))
        .await().atMost(Duration.ofSeconds(5));

    assertEquals("/search-index/_doc/" + document.externalId, path.get());
    assertTrue(body.get().contains("\"externalId\":\"" + document.externalId + "\""));
    assertTrue(body.get().contains("\"tokensHash\":\"tokens-hash\""));
    assertEquals("cmd-1", result.commandId);
    assertEquals(document.externalId, result.externalId);
    assertEquals("search-index", result.indexName);
    assertEquals("UPSERTED", result.resultStatus);
    assertTrue(Boolean.TRUE.equals(result.createdOrUpdated));
  }

  @Test
  void usesInMemorySinkWhenEndpointIsNotConfigured() {
    OpenSearchIndexDocumentCommandConnector connector = new OpenSearchIndexDocumentCommandConnector();

    SearchIndexWriteResult result = (SearchIndexWriteResult) connector
        .execute(request(document()))
        .await().atMost(Duration.ofSeconds(5));

    assertEquals(1, connector.inMemorySinkSize());
    assertEquals("cmd-1", result.commandId);
    assertEquals("search-index", result.indexName);
  }

  @Test
  void returnsDtoResultForDtoInput() {
    SearchIndexDocument document = document();
    SearchIndexDocumentDto dto = SearchIndexDocumentDto.builder()
        .docId(document.docId)
        .externalId(document.externalId)
        .batchIndex(document.batchIndex)
        .tokenCount(document.tokenCount)
        .tokens(document.tokens)
        .tokensHash(document.tokensHash)
        .contentHash(document.contentHash)
        .vectorHash(document.vectorHash)
        .vectorVersion(document.vectorVersion)
        .indexName(document.indexName)
        .build();

    Object result = new OpenSearchIndexDocumentCommandConnector()
        .execute(new CommandRequest<>(
            descriptor(),
            "cmd-1",
            dto,
            new AwaitExecutionContext("tenant", "exec-1", 4),
            Map.of()))
        .await().atMost(Duration.ofSeconds(5));

    assertTrue(result instanceof SearchIndexWriteResultDto);
    assertEquals("cmd-1", ((SearchIndexWriteResultDto) result).getCommandId());
  }

  @Test
  void failsWhenOpenSearchReturnsErrorStatus() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      exchange.sendResponseHeaders(500, 0);
      exchange.getResponseBody().close();
    });
    server.start();
    System.setProperty("search.index.opensearch.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());

    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> new OpenSearchIndexDocumentCommandConnector()
            .execute(request(document()))
            .await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("OpenSearch index write failed"));
  }

  @Test
  void rejectsInvalidCommandDocumentBeforeDispatch() {
    SearchIndexDocument document = document();
    document.externalId = " ";

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new OpenSearchIndexDocumentCommandConnector()
            .execute(request(document))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("externalId is required", error.getMessage());
  }

  private CommandRequest<Object> request(Object document) {
    return new CommandRequest<>(
        descriptor(),
        "cmd-1",
        document,
        new AwaitExecutionContext("tenant", "exec-1", 4),
        Map.of());
  }

  private CommandDescriptor descriptor() {
    return new CommandDescriptor(
        "ProcessWriteSearchIndexDocumentService",
        OpenSearchIndexDocumentCommandConnector.COMMAND,
        SearchIndexDocument.class.getName(),
        SearchIndexWriteResult.class.getName(),
        SearchIndexDocumentCommandIdGenerator.class.getName(),
        CommandDuplicatePolicy.RETURN_RECORDED,
        Map.of());
  }

  private SearchIndexDocument document() {
    SearchIndexDocument document = new SearchIndexDocument();
    document.docId = UUID.randomUUID();
    document.externalId = document.docId + ":0:vec-v1:vector-hash";
    document.batchIndex = 0;
    document.tokenCount = 2;
    document.tokens = "alpha beta";
    document.tokensHash = "tokens-hash";
    document.contentHash = "content-hash";
    document.vectorHash = "vector-hash";
    document.vectorVersion = "vec-v1";
    document.indexName = "search-index";
    return document;
  }
}
