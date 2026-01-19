package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.IndexAckDto;

@ApplicationScoped
@Unremovable
public class IndexCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String tokensHash;
    String indexVersion;
    if (item instanceof IndexAck ack) {
      tokensHash = ack.tokensHash;
      indexVersion = ack.indexVersion;
    } else if (item instanceof IndexAckDto dto) {
      tokensHash = dto.getTokensHash();
      indexVersion = dto.getIndexVersion();
    } else {
      return Optional.empty();
    }
    if (tokensHash == null || tokensHash.isBlank()) {
      return Optional.empty();
    }
    indexVersion = normalize(indexVersion);
    if (indexVersion == null) {
      indexVersion = resolveIndexVersion();
    }
    return Optional.of(IndexAck.class.getName() + ":" + tokensHash.trim() + ":schema=" + indexVersion);
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
