package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@ApplicationScoped
@Unremovable
public class TokenizeCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String contentHash;
    if (item instanceof TokenBatch batch) {
      contentHash = batch.contentHash;
    } else if (item instanceof TokenBatchDto dto) {
      contentHash = dto.getContentHash();
    } else {
      return Optional.empty();
    }
    if (contentHash == null || contentHash.isBlank()) {
      return Optional.empty();
    }
    String modelVersion = resolveModelVersion();
    return Optional.of(TokenBatch.class.getName() + ":" + contentHash.trim() + ":model=" + modelVersion);
  }

  @Override
  public int priority() {
    return 60;
  }

  private String resolveModelVersion() {
    String configured = System.getenv("TOKENIZER_MODEL_VERSION");
    if (configured == null || configured.isBlank()) {
      return "v1";
    }
    return configured.trim();
  }
}
