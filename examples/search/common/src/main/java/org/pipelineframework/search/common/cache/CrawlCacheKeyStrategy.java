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

  /**
   * Computes a cache key for crawl-related items when a stable source URL is available.
   *
   * <p>Supports inputs of type RawDocument, RawDocumentDto, CrawlRequest, and CrawlRequestDto.
   * If a non-blank source URL can be determined, returns an Optional containing a key with the
   * form "{@code <RawDocument-class-name>:<trimmed-sourceUrl>}" and, when fetch options are present,
   * appended as "{@code |<trimmed-fetchOptions>}". Returns {@code Optional.empty()} if the item
   * type is unsupported or the source URL is null or blank.
   *
   * @param item the item to derive a cache key from (one of RawDocument, RawDocumentDto,
   *             CrawlRequest, CrawlRequestDto)
   * @return an {@code Optional} containing the constructed cache key if available,
   *         {@code Optional.empty()} otherwise
   */
  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String sourceUrl;
    String fetchOptions;
      switch (item) {
          case RawDocument document -> {
              sourceUrl = document.sourceUrl;
              fetchOptions = document.fetchOptions;
          }
          case RawDocumentDto dto -> {
              sourceUrl = dto.getSourceUrl();
              fetchOptions = dto.getFetchOptions();
          }
          case CrawlRequest request -> {
              sourceUrl = request.sourceUrl;
              fetchOptions = FetchOptionsNormalizer.normalize(request);
          }
          case CrawlRequestDto dto -> {
              sourceUrl = dto.getSourceUrl();
              fetchOptions = FetchOptionsNormalizer.normalize(dto);
          }
          case null, default -> {
              return Optional.empty();
          }
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

  /**
   * Provides this strategy's ordering priority among cache key strategies.
   *
   * @return the priority value for ordering strategies; higher values indicate higher precedence
   */
  @Override
  public int priority() {
    return 60;
  }

  /**
   * Indicates whether this strategy can compute cache keys for the given target type.
   *
   * @param targetType the target class to check support for
   * @return `true` if the target type is {@code RawDocument}, {@code RawDocumentDto}, {@code CrawlRequest},
   *         or {@code CrawlRequestDto}, `false` otherwise
   */
  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == RawDocument.class
        || targetType == RawDocumentDto.class
        || targetType == CrawlRequest.class
        || targetType == CrawlRequestDto.class;
  }
}
