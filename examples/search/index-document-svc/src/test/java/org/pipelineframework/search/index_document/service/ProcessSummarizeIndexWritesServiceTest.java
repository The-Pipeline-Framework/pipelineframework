package org.pipelineframework.search.index_document.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.util.HashingUtils;

class ProcessSummarizeIndexWritesServiceTest {
  @Test
  void summarizesRecordedWritesIntoIndexAck() {
    UUID docId = UUID.randomUUID();
    SearchIndexWriteResult second = result(docId, 1, "beta gamma", "vh-b");
    SearchIndexWriteResult first = result(docId, 0, "alpha beta alpha", "vh-a");

    IndexAck ack = new ProcessSummarizeIndexWritesService()
        .process(Multi.createFrom().items(second, first))
        .await().atMost(Duration.ofSeconds(5));

    assertEquals(docId, ack.docId);
    assertEquals("search-index", ack.getIndexVersion());
    assertEquals(2, ack.getTokenBatchCount());
    assertEquals(3, ack.getUniqueTokenCount());
    assertEquals("alpha", ack.getTopToken());
    assertEquals(HashingUtils.sha256Base64Url("vh-a|vh-b"), ack.getTokensHash());
    assertTrue(Boolean.TRUE.equals(ack.getSuccess()));
  }

  @Test
  void rejectsMixedDocIds() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new ProcessSummarizeIndexWritesService()
            .process(Multi.createFrom().items(
                result(UUID.randomUUID(), 0, "alpha", "vh-a"),
                result(UUID.randomUUID(), 1, "beta", "vh-b")))
            .await().atMost(Duration.ofSeconds(5)));

    assertTrue(error.getMessage().contains("same docId"));
  }

  private SearchIndexWriteResult result(UUID docId, int batchIndex, String tokens, String vectorHash) {
    SearchIndexWriteResult result = new SearchIndexWriteResult();
    result.docId = docId;
    result.externalId = docId + ":" + batchIndex;
    result.commandId = "cmd-" + batchIndex;
    result.indexName = "search-index";
    result.resultStatus = "UPSERTED";
    result.createdOrUpdated = true;
    result.recordedDuplicate = false;
    result.batchIndex = batchIndex;
    result.tokenCount = tokens.split("\\s+").length;
    result.tokens = tokens;
    result.tokensHash = "tokens-" + batchIndex;
    result.vectorHash = vectorHash;
    result.vectorVersion = "vec-v1";
    return result;
  }
}
