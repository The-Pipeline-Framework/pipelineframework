package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@ApplicationScoped
@Unremovable
public class TokenizeCacheKeyStrategy implements CacheKeyStrategy {

  /**
   * Builds an optional cache key for tokenized content when the input item contains a content hash.
   *
   * The method recognizes items of type TokenBatch, TokenBatchDto, ParsedDocument, and ParsedDocumentDto.
   * If the item's content hash is present and not blank, the returned key uses the TokenBatch class name,
   * the trimmed content hash, and the tokenizer model version in the format:
   *     {fully-qualified-TokenBatch-class-name}:{trimmedContentHash}:model={modelVersion}
   *
   * @param item the runtime object to extract a content hash from; supported types are TokenBatch, TokenBatchDto,
   *             ParsedDocument, and ParsedDocumentDto
   * @return an Optional containing the constructed cache key when a non-blank content hash is available, or an empty Optional otherwise
   */
  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String contentHash;
    if (item instanceof TokenBatch batch) {
      contentHash = batch.contentHash;
    } else if (item instanceof TokenBatchDto dto) {
      contentHash = dto.getContentHash();
    } else if (item instanceof ParsedDocument document) {
      contentHash = document.contentHash;
    } else if (item instanceof ParsedDocumentDto dto) {
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

  /**
   * Priority value used to order this CacheKeyStrategy among others.
   *
   * @return the priority value (60) used to sort strategies; higher numbers indicate higher priority
   */
  @Override
  public int priority() {
    return 60;
  }

  /**
   * Checks whether the given target type is supported by this cache key strategy.
   *
   * @param targetType the target class to check
   * @return {@code true} if {@code targetType} is {@code TokenBatch.class} or {@code TokenBatchDto.class}, {@code false} otherwise
   */
  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == TokenBatch.class || targetType == TokenBatchDto.class;
  }

  /**
   * Determine the tokenizer model version to use.
   *
   * @return the trimmed value of the {@code TOKENIZER_MODEL_VERSION} environment variable if it is set and not blank; otherwise {@code "v1"}.
   */
  private String resolveModelVersion() {
    String configured = System.getenv("TOKENIZER_MODEL_VERSION");
    if (configured == null || configured.isBlank()) {
      return "v1";
    }
    return configured.trim();
  }
}