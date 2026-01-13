package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.ParsedDocument;

@ApplicationScoped
@Unremovable
public class ParseCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    if (!(item instanceof ParsedDocument document)) {
      return Optional.empty();
    }
    if (document.rawContentHash == null || document.rawContentHash.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(document.getClass().getName() + ":" + document.rawContentHash.trim());
  }

  @Override
  public int priority() {
    return 60;
  }
}
