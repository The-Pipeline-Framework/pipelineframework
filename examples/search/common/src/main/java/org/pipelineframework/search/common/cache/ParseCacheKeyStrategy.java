package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;

@ApplicationScoped
@Unremovable
public class ParseCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String rawContentHash;
    if (item instanceof ParsedDocument document) {
      rawContentHash = document.rawContentHash;
    } else if (item instanceof ParsedDocumentDto dto) {
      rawContentHash = dto.getRawContentHash();
    } else {
      return Optional.empty();
    }
    if (rawContentHash == null || rawContentHash.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(ParsedDocument.class.getName() + ":" + rawContentHash.trim());
  }

  @Override
  public int priority() {
    return 60;
  }
}
