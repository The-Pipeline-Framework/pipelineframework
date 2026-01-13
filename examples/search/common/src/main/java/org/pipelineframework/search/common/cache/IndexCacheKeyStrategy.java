package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.IndexAck;

@ApplicationScoped
@Unremovable
public class IndexCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    if (!(item instanceof IndexAck ack)) {
      return Optional.empty();
    }
    if (ack.tokensHash == null || ack.tokensHash.isBlank()) {
      return Optional.empty();
    }
    String indexVersion = normalize(ack.indexVersion);
    if (indexVersion == null) {
      indexVersion = resolveIndexVersion();
    }
    return Optional.of(ack.getClass().getName() + ":" + ack.tokensHash.trim() + ":schema=" + indexVersion);
  }

  @Override
  public int priority() {
    return 60;
  }

  private String resolveIndexVersion() {
    String configured = System.getenv("SEARCH_INDEX_VERSION");
    if (configured == null || configured.isBlank()) {
      return "v1";
    }
    return configured.trim();
  }

  private String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }
}
