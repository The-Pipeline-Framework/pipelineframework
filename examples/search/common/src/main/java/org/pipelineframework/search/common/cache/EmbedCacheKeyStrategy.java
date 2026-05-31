package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@ApplicationScoped
@Unremovable
public class EmbedCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    String docId;
    Integer batchIndex;
    String tokensHash;
    String vectorVersion = null;
    if (item instanceof EmbeddedChunk chunk) {
      docId = chunk.docId == null ? null : chunk.docId.toString();
      batchIndex = chunk.batchIndex;
      tokensHash = chunk.tokensHash;
      vectorVersion = chunk.vectorVersion;
    } else if (item instanceof EmbeddedChunkDto dto) {
      docId = dto.getDocId() == null ? null : dto.getDocId().toString();
      batchIndex = dto.getBatchIndex();
      tokensHash = dto.getTokensHash();
      vectorVersion = dto.getVectorVersion();
    } else if (item instanceof TokenBatch batch) {
      docId = batch.docId == null ? null : batch.docId.toString();
      batchIndex = batch.batchIndex;
      tokensHash = batch.tokensHash;
    } else if (item instanceof TokenBatchDto dto) {
      docId = dto.getDocId() == null ? null : dto.getDocId().toString();
      batchIndex = dto.getBatchIndex();
      tokensHash = dto.getTokensHash();
    } else {
      return Optional.empty();
    }
    if (docId == null || docId.isBlank() || batchIndex == null || batchIndex < 0
        || tokensHash == null || tokensHash.isBlank()) {
      return Optional.empty();
    }
    vectorVersion = normalize(vectorVersion);
    if (vectorVersion == null) {
      vectorVersion = resolveVectorVersion();
    }
    return Optional.of(EmbeddedChunk.class.getName()
        + ":doc=" + docId.trim()
        + ":batch=" + batchIndex
        + ":tokens=" + tokensHash.trim()
        + ":vector=" + vectorVersion);
  }

  @Override
  public int priority() {
    return 60;
  }

  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == EmbeddedChunk.class || targetType == EmbeddedChunkDto.class;
  }

  private String resolveVectorVersion() {
    String configured = System.getenv("SEARCH_EMBED_VECTOR_VERSION");
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
