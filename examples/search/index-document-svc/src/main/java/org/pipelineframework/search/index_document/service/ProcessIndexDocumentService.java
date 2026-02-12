package org.pipelineframework.search.index_document.service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.util.HashingUtils;
import org.pipelineframework.service.ReactiveStreamingClientService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.TokenBatch.class,
    outputType = org.pipelineframework.search.common.domain.IndexAck.class,
    stepType = org.pipelineframework.step.StepManyToOne.class,
    backendType = org.pipelineframework.grpc.GrpcServiceClientStreamingAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.TokenBatchMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.IndexAckMapper.class,
    cacheKeyGenerator = org.pipelineframework.search.index_document.cache.IndexDocumentCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessIndexDocumentService
    implements ReactiveStreamingClientService<TokenBatch, IndexAck> {
  private static final int MAX_BATCHES = 10_000;

  @Override
  public Uni<IndexAck> process(Multi<TokenBatch> input) {
    Logger logger = Logger.getLogger(getClass());
    return input
        .collect()
        .asList()
        .onItem()
        .transformToUni(batches -> processBatches(batches, logger));
  }

  private Uni<IndexAck> processBatches(List<TokenBatch> batches, Logger logger) {
    if (batches == null || batches.isEmpty()) {
      return Uni.createFrom().failure(new IllegalArgumentException("token batches are required"));
    }
    if (batches.size() > MAX_BATCHES) {
      return Uni.createFrom()
          .failure(new IllegalArgumentException("token batch count exceeds limit: " + MAX_BATCHES));
    }
    if (batches.stream().anyMatch(batch ->
        batch == null
            || batch.tokens == null
            || batch.tokens.isBlank()
            || batch.tokensHash == null
            || batch.tokensHash.isBlank())) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "all token batches must contain tokens and tokensHash"));
    }
    UUID docId = batches.get(0).docId;
    if (docId == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("docId is required"));
    }
    if (batches.stream().anyMatch(batch -> batch.docId == null || !docId.equals(batch.docId))) {
      return Uni.createFrom().failure(new IllegalArgumentException("all token batches must share the same docId"));
    }

    String indexVersion = resolveIndexVersion();
    String joinedTokenHashes = batches.stream()
        .map(batch -> batch.tokensHash)
        .collect(Collectors.joining("|"));
    String combinedTokensHash = HashingUtils.sha256Base64Url(joinedTokenHashes);

    IndexAck output = new IndexAck();
    output.docId = docId;
    output.indexVersion = indexVersion;
    output.tokensHash = combinedTokensHash;
    output.indexedAt = Instant.now();
    output.success = true;

    logger.infof("Indexed doc %s from %s token batches (version=%s)", docId, batches.size(), indexVersion);
    return Uni.createFrom().item(output);
  }

  private String resolveIndexVersion() {
    String configured = System.getenv("SEARCH_INDEX_VERSION");
    if (configured == null || configured.isBlank()) {
      return "v1";
    }
    return configured.trim();
  }
}
