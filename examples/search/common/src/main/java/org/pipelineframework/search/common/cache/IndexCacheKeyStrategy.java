package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.IndexAckDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@ApplicationScoped
@Unremovable
public class IndexCacheKeyStrategy implements CacheKeyStrategy {

  /**
   * Derives a cache key string from an index-related item and pipeline context.
   *
   * <p>Supports items of types IndexAck, IndexAckDto, TokenBatch, and TokenBatchDto. If a tokens hash
   * is present the method normalizes and (when necessary) resolves an index version, then returns a
   * key in the form:
   * <pre>
   * &lt;IndexAck class full name&gt;:&lt;tokensHash&gt;:schema=&lt;indexVersion&gt;
   * </pre>
   * If the item type is unsupported or the tokens hash is missing or blank, the method returns
   * {@code Optional.empty()}.
   *
   * @param item the object to derive the key from; expected types: IndexAck, IndexAckDto, TokenBatch, or TokenBatchDto
   * @param context the pipeline context (may be used for resolution in implementations)
   * @return an {@code Optional} containing the derived key string when available, {@code Optional.empty()} otherwise
   */
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
    } else if (item instanceof TokenBatch batch) {
      tokensHash = batch.tokensHash;
      indexVersion = null;
    } else if (item instanceof TokenBatchDto dto) {
      tokensHash = dto.getTokensHash();
      indexVersion = null;
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

  /**
   * Defines the execution priority used to order cache key strategies.
   *
   * @return the priority value used to order cache key strategies
   */
  @Override
  public int priority() {
    return 60;
  }

  /**
   * Indicates whether this strategy supports generating cache keys for the given target type.
   *
   * @param targetType the class to check for support
   * @return `true` if the target type is `IndexAck` or `IndexAckDto`, `false` otherwise
   */
  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == IndexAck.class || targetType == IndexAckDto.class;
  }

  /**
   * Determine the search index schema version by reading the SEARCH_INDEX_VERSION environment variable.
   *
   * @return the trimmed value of SEARCH_INDEX_VERSION, or "v1" if the environment variable is unset or blank
   */
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