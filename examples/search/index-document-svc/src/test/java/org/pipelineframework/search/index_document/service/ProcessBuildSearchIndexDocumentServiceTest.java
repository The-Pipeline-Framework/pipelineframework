package org.pipelineframework.search.index_document.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.SearchIndexDocument;

class ProcessBuildSearchIndexDocumentServiceTest {
  @Test
  void buildsDeterministicOpenSearchDocumentIntent() {
    UUID docId = UUID.randomUUID();
    EmbeddedChunk chunk = chunk(docId);

    SearchIndexDocument document = new ProcessBuildSearchIndexDocumentService("search-index")
        .process(chunk)
        .await().atMost(Duration.ofSeconds(5));

    assertEquals(docId, document.docId);
    assertEquals("search-index", document.indexName);
    assertEquals(docId + ":2:vec-v1:vector-hash", document.externalId);
    assertEquals(chunk.tokensHash, document.tokensHash);
    assertEquals(chunk.vectorHash, document.vectorHash);
  }

  @Test
  void rejectsMissingVectorHash() {
    EmbeddedChunk chunk = chunk(UUID.randomUUID());
    chunk.vectorHash = " ";

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new ProcessBuildSearchIndexDocumentService("search-index")
            .process(chunk)
            .await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("vectorHash"));
  }

  private EmbeddedChunk chunk(UUID docId) {
    EmbeddedChunk chunk = new EmbeddedChunk();
    chunk.docId = docId;
    chunk.batchIndex = 2;
    chunk.tokenCount = 3;
    chunk.tokens = "alpha beta alpha";
    chunk.tokensHash = "tokens-hash";
    chunk.contentHash = "content-hash";
    chunk.vectorVersion = "vec-v1";
    chunk.vectorHash = "vector-hash";
    return chunk;
  }
}
