package org.pipelineframework.search.common.command;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;
import org.pipelineframework.search.common.dto.SearchIndexWriteResultDto;

@ApplicationScoped
public class OpenSearchIndexDocumentCommandConnector
    implements CommandConnector<Object, Object> {
  public static final String COMMAND = "opensearch-index-document";

  private final Map<String, IndexedDocument> inMemorySink = new ConcurrentHashMap<>();
  private final HttpClient httpClient = HttpClient.newHttpClient();
  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

  @Override
  public String command() {
    return COMMAND;
  }

  @Override
  public Uni<Object> execute(CommandRequest<Object> request) {
    return Uni.createFrom().item(() -> executeBlocking(request))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  public int inMemorySinkSize() {
    return inMemorySink.size();
  }

  private Object executeBlocking(CommandRequest<Object> request) {
    IndexedDocument document = validate(request.input());
    String indexName = resolveIndexName(document);
    Optional<String> endpoint = config("search.index.opensearch.endpoint");
    if (endpoint.isPresent()) {
      putOpenSearchDocument(endpoint.get(), indexName, document);
    } else {
      inMemorySink.put(document.externalId(), document);
    }
    return result(request, document, indexName, "UPSERTED", true, false);
  }

  private void putOpenSearchDocument(String endpoint, String indexName, IndexedDocument document) {
    try {
      String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
      URI uri = URI.create(base + "/" + indexName + "/_doc/" + document.externalId());
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("docId", document.docId().toString());
      payload.put("externalId", document.externalId());
      payload.put("batchIndex", document.batchIndex());
      payload.put("tokenCount", document.tokenCount());
      payload.put("tokens", document.tokens());
      payload.put("tokensHash", document.tokensHash());
      payload.put("contentHash", document.contentHash());
      payload.put("vectorHash", document.vectorHash());
      payload.put("vectorVersion", document.vectorVersion());
      String body = objectMapper.writeValueAsString(payload);
      HttpRequest request = HttpRequest.newBuilder(uri)
          .timeout(Duration.ofSeconds(timeoutSeconds()))
          .header("content-type", "application/json")
          .PUT(HttpRequest.BodyPublishers.ofString(body))
          .build();
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalStateException("OpenSearch index write failed with HTTP " + response.statusCode());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("OpenSearch index write interrupted", e);
    } catch (Exception e) {
      throw new IllegalStateException("OpenSearch index write failed", e);
    }
  }

  private Object result(
      CommandRequest<Object> request,
      IndexedDocument document,
      String indexName,
      String status,
      boolean createdOrUpdated,
      boolean recordedDuplicate) {
    if (request.input() instanceof SearchIndexDocumentDto) {
      return SearchIndexWriteResultDto.builder()
          .docId(document.docId())
          .externalId(document.externalId())
          .commandId(request.commandId())
          .indexName(indexName)
          .resultStatus(status)
          .createdOrUpdated(createdOrUpdated)
          .recordedDuplicate(recordedDuplicate)
          .batchIndex(document.batchIndex())
          .tokenCount(document.tokenCount())
          .tokens(document.tokens())
          .tokensHash(document.tokensHash())
          .vectorHash(document.vectorHash())
          .vectorVersion(document.vectorVersion())
          .indexedAt(Instant.now())
          .build();
    }
    SearchIndexWriteResult result = new SearchIndexWriteResult();
    result.docId = document.docId();
    result.externalId = document.externalId();
    result.commandId = request.commandId();
    result.indexName = indexName;
    result.resultStatus = status;
    result.createdOrUpdated = createdOrUpdated;
    result.recordedDuplicate = recordedDuplicate;
    result.batchIndex = document.batchIndex();
    result.tokenCount = document.tokenCount();
    result.tokens = document.tokens();
    result.tokensHash = document.tokensHash();
    result.vectorHash = document.vectorHash();
    result.vectorVersion = document.vectorVersion();
    result.indexedAt = Instant.now();
    return result;
  }

  private IndexedDocument validate(Object input) {
    IndexedDocument document;
    if (input instanceof SearchIndexDocument domain) {
      document = new IndexedDocument(
          domain.docId,
          domain.externalId,
          domain.batchIndex,
          domain.tokenCount,
          domain.tokens,
          domain.tokensHash,
          domain.contentHash,
          domain.vectorHash,
          domain.vectorVersion,
          domain.indexName);
    } else if (input instanceof SearchIndexDocumentDto dto) {
      document = new IndexedDocument(
          dto.getDocId(),
          dto.getExternalId(),
          dto.getBatchIndex(),
          dto.getTokenCount(),
          dto.getTokens(),
          dto.getTokensHash(),
          dto.getContentHash(),
          dto.getVectorHash(),
          dto.getVectorVersion(),
          dto.getIndexName());
    } else if (input == null) {
      throw new IllegalArgumentException("search index document is required");
    } else {
      throw new IllegalArgumentException("unsupported search index document type: " + input.getClass().getName());
    }
    if (document.docId() == null) {
      throw new IllegalArgumentException("docId is required");
    }
    if (document.externalId() == null || document.externalId().isBlank()) {
      throw new IllegalArgumentException("externalId is required");
    }
    if (document.batchIndex() == null || document.batchIndex() < 0) {
      throw new IllegalArgumentException("batchIndex must be >= 0");
    }
    if (document.vectorHash() == null || document.vectorHash().isBlank()) {
      throw new IllegalArgumentException("vectorHash is required");
    }
    if (document.vectorVersion() == null || document.vectorVersion().isBlank()) {
      throw new IllegalArgumentException("vectorVersion is required");
    }
    return document;
  }

  private String resolveIndexName(IndexedDocument document) {
    if (document.indexName() != null && !document.indexName().isBlank()) {
      return document.indexName().trim();
    }
    return config("search.index.opensearch.index").orElse("search-documents");
  }

  private long timeoutSeconds() {
    return config("search.index.opensearch.timeout-seconds")
        .map(value -> {
          try {
            return Long.parseLong(value);
          } catch (NumberFormatException ignored) {
            return 5L;
          }
        })
        .orElse(5L);
  }

  private Optional<String> config(String key) {
    return ConfigProvider.getConfig().getOptionalValue(key, String.class)
        .filter(value -> !value.isBlank())
        .map(String::trim);
  }

  private record IndexedDocument(
      java.util.UUID docId,
      String externalId,
      Integer batchIndex,
      Integer tokenCount,
      String tokens,
      String tokensHash,
      String contentHash,
      String vectorHash,
      String vectorVersion,
      String indexName) {
  }
}
