package org.pipelineframework.search.index_document.service;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveStreamingClientService;

@PipelineStep
@ApplicationScoped
public class ProcessSummarizeIndexWritesService
    implements ReactiveStreamingClientService<SearchIndexWriteResult, IndexAck> {
  private static final int MAX_WRITES = 10_000;
  private static final Logger LOGGER = Logger.getLogger(ProcessSummarizeIndexWritesService.class);

  @Override
  public Uni<IndexAck> process(Multi<SearchIndexWriteResult> input) {
    return input.collect().asList().onItem().transformToUni(this::summarize);
  }

  private Uni<IndexAck> summarize(List<SearchIndexWriteResult> results) {
    if (results == null || results.isEmpty()) {
      return Uni.createFrom().failure(new IllegalArgumentException("index write results are required"));
    }
    if (results.size() > MAX_WRITES) {
      return Uni.createFrom().failure(new IllegalArgumentException("index write result count exceeds limit: " + MAX_WRITES));
    }
    UUID docId = results.get(0).docId;
    if (docId == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("docId is required"));
    }
    if (results.stream().anyMatch(result -> result == null || result.docId == null || !docId.equals(result.docId))) {
      return Uni.createFrom().failure(new IllegalArgumentException("all index write results must share the same docId"));
    }
    if (results.stream().anyMatch(result ->
        result.externalId == null
            || result.externalId.isBlank()
            || result.commandId == null
            || result.commandId.isBlank()
            || result.indexName == null
            || result.indexName.isBlank()
            || result.batchIndex == null
            || result.vectorHash == null
            || result.vectorHash.isBlank()
            || !Boolean.TRUE.equals(result.createdOrUpdated))) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "all index write results must be successful recorded writes for docId " + docId));
    }

    List<SearchIndexWriteResult> ordered = results.stream()
        .sorted(Comparator
            .comparing((SearchIndexWriteResult result) -> result.batchIndex, Comparator.nullsLast(Integer::compareTo))
            .thenComparing(result -> result.vectorHash, Comparator.nullsLast(String::compareTo))
            .thenComparing(result -> result.externalId, Comparator.nullsLast(String::compareTo)))
        .toList();
    String joinedVectorHashes = ordered.stream()
        .map(result -> result.vectorHash == null ? "" : result.vectorHash)
        .collect(Collectors.joining("|"));
    AggregationSummary summary = summarizeTokens(ordered);

    IndexAck ack = new IndexAck();
    ack.docId = docId;
    ack.setIndexVersion(ordered.get(0).indexName);
    ack.setTokensHash(HashingUtils.sha256Base64Url(joinedVectorHashes));
    ack.setTokenBatchCount(ordered.size());
    ack.setUniqueTokenCount(summary.uniqueTokenCount());
    ack.setTopToken(summary.topToken());
    ack.setIndexedAt(Instant.now());
    ack.setSuccess(true);

    LOGGER.debugf("Summarized %s OpenSearch writes for doc %s", ordered.size(), docId);
    return Uni.createFrom().item(ack);
  }

  private AggregationSummary summarizeTokens(List<SearchIndexWriteResult> results) {
    HashMap<String, Integer> counts = new HashMap<>();
    for (SearchIndexWriteResult result : results) {
      if (result.tokens == null || result.tokens.isBlank()) {
        continue;
      }
      for (String token : result.tokens.trim().split("\\s+")) {
        if (!token.isBlank()) {
          counts.merge(token, 1, Integer::sum);
        }
      }
    }
    String topToken = counts.entrySet().stream()
        .sorted(Comparator
            .<java.util.Map.Entry<String, Integer>>comparingInt(java.util.Map.Entry::getValue)
            .reversed()
            .thenComparing(java.util.Map.Entry::getKey))
        .map(java.util.Map.Entry::getKey)
        .findFirst()
        .orElse("");
    return new AggregationSummary(counts.size(), topToken);
  }

  private record AggregationSummary(int uniqueTokenCount, String topToken) {
  }
}
