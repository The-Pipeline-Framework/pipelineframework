package org.pipelineframework.search.index_document.service;

import java.time.Instant;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = org.pipelineframework.search.common.domain.TokenBatch.class,
    outputType = org.pipelineframework.search.common.domain.IndexAck.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.search.common.mapper.TokenBatchMapper.class,
    outboundMapper = org.pipelineframework.search.common.mapper.IndexAckMapper.class,
    cacheKeyGenerator = org.pipelineframework.search.index_document.cache.IndexDocumentCacheKeyGenerator.class
)
@ApplicationScoped
@Getter
public class ProcessIndexDocumentService
    implements ReactiveService<TokenBatch, IndexAck> {

  @Override
  public Uni<IndexAck> process(TokenBatch input) {
    Logger logger = Logger.getLogger(getClass());

    if (input == null || input.tokens == null || input.tokens.isBlank()) {
      return Uni.createFrom().failure(new IllegalArgumentException("tokens are required"));
    }

    String indexVersion = resolveIndexVersion();

    IndexAck output = new IndexAck();
    output.docId = input.docId;
    output.indexVersion = indexVersion;
    output.indexedAt = Instant.now();
    output.success = true;

    logger.infof("Indexed doc %s (version=%s)", input.docId, indexVersion);
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
