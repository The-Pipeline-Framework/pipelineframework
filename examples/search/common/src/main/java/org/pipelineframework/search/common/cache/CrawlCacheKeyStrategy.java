package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.RawDocument;

@ApplicationScoped
@Unremovable
public class CrawlCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    if (!(item instanceof RawDocument document)) {
      return Optional.empty();
    }
    if (document.sourceUrl == null || document.sourceUrl.isBlank()) {
      return Optional.empty();
    }
    StringBuilder key = new StringBuilder();
    key.append(document.getClass().getName()).append(":").append(document.sourceUrl.trim());
    if (document.fetchOptions != null && !document.fetchOptions.isBlank()) {
      key.append("|").append(document.fetchOptions.trim());
    }
    return Optional.of(key.toString());
  }

  @Override
  public int priority() {
    return 60;
  }
}
