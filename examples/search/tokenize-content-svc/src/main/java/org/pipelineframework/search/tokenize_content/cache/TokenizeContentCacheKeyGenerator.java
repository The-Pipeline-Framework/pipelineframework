package org.pipelineframework.search.tokenize_content.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.ParsedDocument;

@ApplicationScoped
@Unremovable
public class TokenizeContentCacheKeyGenerator implements CacheKeyGenerator {

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
    if (!(target instanceof ParsedDocument document)) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String contentHash = normalize(document.contentHash);
    if (contentHash == null) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String modelVersion = resolveModelVersion();
    return document.getClass().getName() + ":" + contentHash + ":model=" + modelVersion;
  }

  private String resolveModelVersion() {
    String configured = System.getenv("TOKENIZER_MODEL_VERSION");
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
