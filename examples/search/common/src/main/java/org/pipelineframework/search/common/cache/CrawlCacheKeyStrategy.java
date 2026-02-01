package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.dto.CrawlRequestDto;
import org.pipelineframework.search.common.dto.RawDocumentDto;
import org.pipelineframework.search.common.util.FetchOptionsNormalizer;

@ApplicationScoped
@Unremovable
public class CrawlCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String sourceUrl;
    String fetchOptions;
    if (item instanceof RawDocument document) {
      sourceUrl = document.sourceUrl;
      fetchOptions = document.fetchOptions;
    } else if (item instanceof RawDocumentDto dto) {
      sourceUrl = dto.getSourceUrl();
      fetchOptions = dto.getFetchOptions();
    } else if (item instanceof CrawlRequest request) {
      sourceUrl = request.sourceUrl;
      fetchOptions = FetchOptionsNormalizer.normalize(request);
    } else if (item instanceof CrawlRequestDto dto) {
      sourceUrl = dto.getSourceUrl();
      fetchOptions = FetchOptionsNormalizer.normalize(dto);
    } else {
      return Optional.empty();
    }
    if (sourceUrl == null || sourceUrl.isBlank()) {
      return Optional.empty();
    }
    StringBuilder key = new StringBuilder();
    key.append(RawDocument.class.getName()).append(":").append(sourceUrl.trim());
    if (fetchOptions != null && !fetchOptions.isBlank()) {
      key.append("|").append(fetchOptions.trim());
    }
    return Optional.of(key.toString());
  }

  @Override
  public int priority() {
    return 60;
  }

  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == RawDocument.class || targetType == RawDocumentDto.class;
  }
}
