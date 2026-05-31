package org.pipelineframework.search.embed_content.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.TokenBatch;

@ApplicationScoped
@Unremovable
public class EmbedContentCacheKeyGenerator implements CacheKeyGenerator {

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
    if (!(target instanceof TokenBatch batch)) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String tokensHash = normalize(batch.tokensHash);
    String docId = batch.docId == null ? null : batch.docId.toString();
    if (docId == null || batch.batchIndex == null || batch.batchIndex < 0 || tokensHash == null) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String vectorVersion = resolveVectorVersion();
    return batch.getClass().getName()
        + ":doc=" + docId
        + ":batch=" + batch.batchIndex
        + ":tokens=" + tokensHash
        + ":vector=" + vectorVersion;
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
