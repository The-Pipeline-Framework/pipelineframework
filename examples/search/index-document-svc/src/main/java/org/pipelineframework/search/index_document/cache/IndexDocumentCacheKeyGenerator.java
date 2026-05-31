package org.pipelineframework.search.index_document.cache;

import java.lang.reflect.Method;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.EmbeddedChunk;

@ApplicationScoped
@Unremovable
public class IndexDocumentCacheKeyGenerator implements CacheKeyGenerator {

  @Override
  public Object generate(Method method, Object... methodParams) {
    String baseKey = buildBaseKey(methodParams);

    PipelineContext context = PipelineContextHolder.get();
    String versionTag = context != null ? context.versionTag() : null;
    return PipelineCacheKeyFormat.applyVersionTag(baseKey, versionTag);
  }

  private String buildBaseKey(Object... methodParams) {
    if (methodParams == null || methodParams.length == 0) {
      return "no-params";
    }

    Object target = methodParams[0];
    List<EmbeddedChunk> chunks;
    if (target instanceof EmbeddedChunk chunk) {
      chunks = List.of(chunk);
    } else if (target instanceof List<?> list && list.stream().allMatch(EmbeddedChunk.class::isInstance)) {
      chunks = list.stream().map(EmbeddedChunk.class::cast).toList();
    } else {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }
    if (chunks.isEmpty()) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }
    String docId = chunks.getFirst().docId == null ? null : chunks.getFirst().docId.toString();
    if (docId == null || chunks.stream().anyMatch(chunk -> chunk.docId == null || !docId.equals(chunk.docId.toString()))) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }
    if (chunks.stream().map(chunk -> normalize(chunk.vectorHash)).anyMatch(java.util.Objects::isNull)) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }
    String vectorHashes = chunks.stream()
        .sorted(java.util.Comparator.comparing((EmbeddedChunk chunk) -> chunk.batchIndex,
            java.util.Comparator.nullsLast(Integer::compareTo)))
        .map(chunk -> normalize(chunk.vectorHash))
        .reduce((left, right) -> left + "|" + right)
        .orElse(null);
    if (vectorHashes == null || vectorHashes.isBlank()) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String indexVersion = resolveIndexVersion();
    return EmbeddedChunk.class.getName() + ":doc=" + docId + ":vectors=" + vectorHashes + ":schema=" + indexVersion;
  }

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
