package org.pipelineframework.search.parse_document.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.RawDocument;

@ApplicationScoped
@Unremovable
public class ParseDocumentCacheKeyGenerator implements CacheKeyGenerator {

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
    if (!(target instanceof RawDocument document)) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String rawContentHash = normalize(document.rawContentHash);
    if (rawContentHash == null) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    return document.getClass().getName() + ":" + rawContentHash;
  }

  private String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }

}
