package org.pipelineframework.search.tokenize_content.service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.TokenBatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessTokenizeContentServiceTest {

  private static final int EXPECTED_BATCH_SIZE = 4;
  private static final Duration TEST_TIMEOUT = Duration.ofSeconds(1);

  private final ProcessTokenizeContentService service = new ProcessTokenizeContentService();

  @Test
  void fansOutParsedDocumentIntoOrderedTokenBatchesWithCounts() {
    ParsedDocument input = new ParsedDocument();
    input.docId = UUID.randomUUID();
    input.contentHash = "content-hash";
    input.content = "alpha beta gamma delta epsilon zeta eta theta";

    List<TokenBatch> batches = service.process(input).collect().asList().await().atMost(TEST_TIMEOUT);

    assertEquals(2, batches.size());
    assertEquals(0, batches.get(0).batchIndex);
    assertEquals(1, batches.get(1).batchIndex);
    assertEquals(EXPECTED_BATCH_SIZE, batches.get(0).tokenCount);
    assertEquals(EXPECTED_BATCH_SIZE, batches.get(1).tokenCount);
    assertTrue(batches.stream().allMatch(batch -> batch.tokensHash != null && !batch.tokensHash.isBlank()));
    assertTrue(batches.stream().allMatch(batch -> input.docId.equals(batch.docId)));
  }

  @Test
  void returnsNoBatchesWhenContentNormalizesToNoTokens() {
    ParsedDocument input = parsedDocument("the and of in to");

    List<TokenBatch> batches = service.process(input).collect().asList().await().atMost(TEST_TIMEOUT);

    assertEquals(0, batches.size());
  }

  @Test
  void returnsSingleBatchWhenTokenCountIsLessThanBatchSize() {
    ParsedDocument input = parsedDocument("alpha beta gamma");

    List<TokenBatch> batches = service.process(input).collect().asList().await().atMost(TEST_TIMEOUT);

    assertEquals(1, batches.size());
    assertEquals(0, batches.get(0).batchIndex);
    assertEquals(3, batches.get(0).tokenCount);
    assertTrue(batches.get(0).tokensHash != null && !batches.get(0).tokensHash.isBlank());
    assertEquals(input.docId, batches.get(0).docId);
  }

  @Test
  void returnsSingleBoundaryBatchWhenTokenCountEqualsBatchSize() {
    ParsedDocument input = parsedDocument("alpha beta gamma delta");

    List<TokenBatch> batches = service.process(input).collect().asList().await().atMost(TEST_TIMEOUT);

    assertEquals(1, batches.size());
    assertEquals(0, batches.get(0).batchIndex);
    assertEquals(EXPECTED_BATCH_SIZE, batches.get(0).tokenCount);
    assertTrue(batches.get(0).tokensHash != null && !batches.get(0).tokensHash.isBlank());
    assertEquals(input.docId, batches.get(0).docId);
  }

  @Test
  void failsForNullOrBlankContent() {
    ParsedDocument nullContent = parsedDocument(null);
    ParsedDocument blankContent = parsedDocument("   ");

    Throwable nullFailure = assertThrows(
        Throwable.class,
        () -> service.process(nullContent).collect().asList().await().atMost(TEST_TIMEOUT));
    Throwable blankFailure = assertThrows(
        Throwable.class,
        () -> service.process(blankContent).collect().asList().await().atMost(TEST_TIMEOUT));

    assertTrue(rootCauseMessage(nullFailure).contains("content is required"));
    assertTrue(rootCauseMessage(blankFailure).contains("content is required"));
  }

  private ParsedDocument parsedDocument(String content) {
    ParsedDocument input = new ParsedDocument();
    input.docId = UUID.randomUUID();
    input.contentHash = "content-hash";
    input.content = content;
    return input;
  }

  private String rootCauseMessage(Throwable error) {
    Throwable cursor = error;
    while (cursor.getCause() != null) {
      cursor = cursor.getCause();
    }
    return cursor.getMessage() == null ? "" : cursor.getMessage();
  }
}
