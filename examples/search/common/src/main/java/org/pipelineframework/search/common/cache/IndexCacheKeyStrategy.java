package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.IndexAckDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@ApplicationScoped
@Unremovable
public class IndexCacheKeyStrategy implements CacheKeyStrategy {

  /**
   * Derives a cache key string from an index-related item and pipeline context.
   *
   * <p>Supports items of types IndexAck, IndexAckDto, EmbeddedChunk, EmbeddedChunkDto, TokenBatch,
   * and TokenBatchDto. If a document id and tokens hash are present, the method normalizes and
   * (when necessary) resolves an index schema version, then returns a key in the form:
   * <pre>
   * &lt;IndexAck class full name&gt;:doc=&lt;docId&gt;[:batch=&lt;batchIndex&gt;]:tokens=&lt;tokensHash&gt;[:vector=&lt;vectorHash&gt;][:vectorVersion=&lt;version&gt;]:schema=&lt;indexVersion&gt;
   * </pre>
   * If the item type is unsupported, or the document id or tokens hash is missing or blank, the
   * method returns {@code Optional.empty()}.
   *
   * @param item the object to derive the key from; expected types: IndexAck, IndexAckDto,
   *             EmbeddedChunk, EmbeddedChunkDto, TokenBatch, or TokenBatchDto
   * @param context the pipeline context (may be used for resolution in implementations)
   * @return an {@code Optional} containing the derived key string when available, {@code Optional.empty()} otherwise
   */
  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String docId;
    Integer batchIndex = null;
    String tokensHash;
    String vectorHash = null;
    String vectorVersion = null;
    String indexVersion;
    if (item instanceof IndexAck ack) {
      docId = ack.docId == null ? null : ack.docId.toString();
      tokensHash = ack.getTokensHash();
      indexVersion = ack.getIndexVersion();
    } else if (item instanceof IndexAckDto dto) {
      docId = dto.getDocId() == null ? null : dto.getDocId().toString();
      tokensHash = dto.getTokensHash();
      indexVersion = dto.getIndexVersion();
    } else if (item instanceof EmbeddedChunk chunk) {
      docId = chunk.docId == null ? null : chunk.docId.toString();
      batchIndex = chunk.batchIndex;
      tokensHash = chunk.tokensHash;
      vectorHash = chunk.vectorHash;
      vectorVersion = chunk.vectorVersion;
      indexVersion = null;
    } else if (item instanceof EmbeddedChunkDto dto) {
      docId = dto.getDocId() == null ? null : dto.getDocId().toString();
      batchIndex = dto.getBatchIndex();
      tokensHash = dto.getTokensHash();
      vectorHash = dto.getVectorHash();
      vectorVersion = dto.getVectorVersion();
      indexVersion = null;
    } else if (item instanceof TokenBatch batch) {
      docId = batch.docId == null ? null : batch.docId.toString();
      batchIndex = batch.batchIndex;
      tokensHash = batch.tokensHash;
      indexVersion = null;
    } else if (item instanceof TokenBatchDto dto) {
      docId = dto.getDocId() == null ? null : dto.getDocId().toString();
      batchIndex = dto.getBatchIndex();
      tokensHash = dto.getTokensHash();
      indexVersion = null;
    } else {
      return Optional.empty();
    }
    if (docId == null || docId.isBlank() || tokensHash == null || tokensHash.isBlank()) {
      return Optional.empty();
    }
    indexVersion = normalize(indexVersion);
    if (indexVersion == null) {
      indexVersion = resolveIndexVersion();
    }
    StringBuilder key = new StringBuilder(IndexAck.class.getName())
        .append(":doc=").append(docId.trim());
    if (batchIndex != null) {
      if (batchIndex < 0) {
        return Optional.empty();
      }
      key.append(":batch=").append(batchIndex);
    }
    key.append(":tokens=").append(tokensHash.trim());
    vectorHash = normalize(vectorHash);
    if (vectorHash != null) {
      key.append(":vector=").append(vectorHash);
    }
    vectorVersion = normalize(vectorVersion);
    if (vectorVersion != null) {
      key.append(":vectorVersion=").append(vectorVersion);
    }
    key.append(":schema=").append(indexVersion);
    return Optional.of(key.toString());
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
   * Indicates whether this strategy supports generating cache entries for the given target type.
   *
   * <p>{@link #resolveKey(Object, PipelineContext)} also accepts EmbeddedChunk, EmbeddedChunkDto,
   * TokenBatch, and TokenBatchDto inputs so upstream stream elements can resolve to the same
   * IndexAck cache key shape.
   *
   * @param targetType the class to check for support
   * @return `true` if the target type is `IndexAck` or `IndexAckDto`, `false` otherwise
   */
  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == IndexAck.class || targetType == IndexAckDto.class;
  }

  /**
   * Determine the search index schema version by reading the MicroProfile Config property
   * {@code search.index.version}.
   *
   * @return the trimmed configured value, or "v1" if the property is unset or blank
   */
  private String resolveIndexVersion() {
    try {
      String configured = ConfigProvider.getConfig()
          .getOptionalValue("search.index.version", String.class)
          .orElse(null);
      if (configured == null || configured.isBlank()) {
        return "v1";
      }
      return configured.trim();
    } catch (IllegalStateException error) {
      return "v1";
    }
  }

  private String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }
}
