package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.dto.RawDocumentDto;

@ApplicationScoped
@Unremovable
public class ParseCacheKeyStrategy implements CacheKeyStrategy {

  /**
   * Builds a cache key from an item's raw content hash when the item is a supported document type.
   *
   * @param item the object to inspect; supported types are ParsedDocument, ParsedDocumentDto, RawDocument, and RawDocumentDto
   * @param context the pipeline context (not used to build the key)
   * @return an Optional containing "fully.qualified.ParsedDocumentClassName:trimmedRawContentHash" when the item is supported and its raw content hash is non-blank, or an empty Optional otherwise
   */
  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String rawContentHash;
    if (item instanceof ParsedDocument document) {
      rawContentHash = document.rawContentHash;
    } else if (item instanceof ParsedDocumentDto dto) {
      rawContentHash = dto.getRawContentHash();
    } else if (item instanceof RawDocument document) {
      rawContentHash = document.rawContentHash;
    } else if (item instanceof RawDocumentDto dto) {
      rawContentHash = dto.getRawContentHash();
    } else {
      return Optional.empty();
    }
    if (rawContentHash == null || rawContentHash.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(ParsedDocument.class.getName() + ":" + rawContentHash.trim());
  }

  /**
   * Defines this strategy's execution priority among CacheKeyStrategy implementations.
   *
   * @return the priority value (60); higher values indicate higher precedence when ordering strategies
   */
  @Override
  public int priority() {
    return 60;
  }

  /**
   * Indicates whether this strategy supports producing cache keys for the given target type.
   *
   * @param targetType the target class to check support for
   * @return {@code true} if the target type is {@link ParsedDocument} or {@link ParsedDocumentDto}, {@code false} otherwise
   */
  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == ParsedDocument.class || targetType == ParsedDocumentDto.class;
  }
}