package org.pipelineframework.search.common.cache;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;
import org.pipelineframework.search.common.dto.SearchIndexWriteResultDto;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchIndexWriteResultCacheKeyStrategyTest {

  @Test
  void derivesTheSameKeyFromTheCommandInputAndResult() {
    UUID docId = UUID.fromString("00000000-0000-0000-0000-000000000006");
    SearchIndexDocumentDto input = SearchIndexDocumentDto.builder()
        .docId(docId)
        .externalId("document-3")
        .batchIndex(2)
        .tokensHash("tokens")
        .vectorHash("vector")
        .vectorVersion("model-v1")
        .indexName("search-documents")
        .build();
    SearchIndexWriteResultDto output = SearchIndexWriteResultDto.builder()
        .docId(docId)
        .externalId("document-3")
        .batchIndex(2)
        .tokensHash("tokens")
        .vectorHash("vector")
        .vectorVersion("model-v1")
        .indexName("search-documents")
        .build();
    SearchIndexWriteResultCacheKeyStrategy strategy = new SearchIndexWriteResultCacheKeyStrategy();

    String expected = SearchIndexWriteResult.class.getName()
        + ":doc=" + docId + ":external=document-3"
        + ":batch=2:tokens=tokens:vector=vector:vectorVersion=model-v1:index=search-documents";
    assertEquals(expected, strategy.resolveKey(input, null).orElseThrow());
    assertEquals(expected, strategy.resolveKey(output, null).orElseThrow());
  }
}
