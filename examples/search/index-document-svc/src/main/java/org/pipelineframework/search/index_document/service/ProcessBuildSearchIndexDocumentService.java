package org.pipelineframework.search.index_document.service;

import java.time.Instant;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.service.ReactiveService;

@PipelineStep
@ApplicationScoped
public class ProcessBuildSearchIndexDocumentService
    implements ReactiveService<EmbeddedChunk, SearchIndexDocument> {
  private static final Logger LOGGER = Logger.getLogger(ProcessBuildSearchIndexDocumentService.class);

  private final String indexName;

  @Inject
  public ProcessBuildSearchIndexDocumentService(
      @ConfigProperty(name = "search.index.opensearch.index", defaultValue = "search-documents") String indexName) {
    this.indexName = indexName == null || indexName.isBlank() ? "search-documents" : indexName.trim();
  }

  @Override
  public Uni<SearchIndexDocument> process(EmbeddedChunk input) {
    return Uni.createFrom().item(() -> build(input));
  }

  private SearchIndexDocument build(EmbeddedChunk input) {
    if (input == null) {
      throw new IllegalArgumentException("embedded chunk is required");
    }
    if (input.docId == null) {
      throw new IllegalArgumentException("docId is required");
    }
    if (input.batchIndex == null || input.batchIndex < 0) {
      throw new IllegalArgumentException("batchIndex must be >= 0");
    }
    if (input.vectorVersion == null || input.vectorVersion.isBlank()) {
      throw new IllegalArgumentException("vectorVersion is required");
    }
    if (input.vectorHash == null || input.vectorHash.isBlank()) {
      throw new IllegalArgumentException("vectorHash is required");
    }

    SearchIndexDocument output = new SearchIndexDocument();
    output.docId = input.docId;
    output.externalId = externalId(input);
    output.batchIndex = input.batchIndex;
    output.tokenCount = input.tokenCount;
    output.tokens = input.tokens;
    output.tokensHash = input.tokensHash;
    output.contentHash = input.contentHash;
    output.vectorHash = input.vectorHash;
    output.vectorVersion = input.vectorVersion;
    output.indexName = indexName;
    output.preparedAt = Instant.now();

    LOGGER.debugf("Prepared OpenSearch index document %s for doc %s batch %s",
        output.externalId, output.docId, output.batchIndex);
    return output;
  }

  private String externalId(EmbeddedChunk input) {
    return input.docId + ":" + input.batchIndex + ":" + input.vectorVersion.trim() + ":" + input.vectorHash.trim();
  }
}
