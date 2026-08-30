package org.pipelineframework.search.common.cache;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchIndexDocumentCacheKeyStrategyTest {

  @Test
  void derivesTheSameKeyFromTheStepInputAndOutput() {
    String propertyName = "search.index.opensearch.index";
    String previousIndexName = System.getProperty(propertyName);
    System.setProperty(propertyName, "search-documents");
    try {
      UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000005");
      EmbeddedChunkDto input = EmbeddedChunkDto.builder()
          .docId(docId)
          .batchIndex(2)
          .tokensHash("tokens")
          .vectorHash("vector")
          .vectorVersion("model-v1")
          .build();
      SearchIndexDocumentDto output = SearchIndexDocumentDto.builder()
          .docId(docId)
          .batchIndex(2)
          .tokensHash("tokens")
          .vectorHash("vector")
          .vectorVersion("model-v1")
          .indexName("search-documents")
          .externalId("document-2")
          .build();
      SearchIndexDocumentCacheKeyStrategy strategy = new SearchIndexDocumentCacheKeyStrategy();

      String expected = SearchIndexDocument.class.getName()
          + ":doc=" + docId
          + ":batch=2:tokens=tokens:vector=vector:vectorVersion=model-v1:index=search-documents";
      assertEquals(expected, strategy.resolveKey(input, null).orElseThrow());
      assertEquals(expected, strategy.resolveKey(output, null).orElseThrow());
    } finally {
      if (previousIndexName == null) {
        System.clearProperty(propertyName);
      } else {
        System.setProperty(propertyName, previousIndexName);
      }
    }
  }
}
