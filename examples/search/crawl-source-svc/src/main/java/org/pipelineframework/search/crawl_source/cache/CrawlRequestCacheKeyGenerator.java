package org.pipelineframework.search.crawl_source.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.crawl_source.service.FetchOptionsNormalizer;

@ApplicationScoped
@Unremovable
public class CrawlRequestCacheKeyGenerator implements CacheKeyGenerator {

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
    if (!(target instanceof CrawlRequest request)) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String sourceUrl = normalize(request.sourceUrl);
    if (sourceUrl == null) {
      return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
    }

    String fetchOptions = FetchOptionsNormalizer.normalize(request);
    StringBuilder key = new StringBuilder();
    key.append(request.getClass().getName()).append(":").append(sourceUrl);
    if (fetchOptions != null) {
      key.append("|").append(fetchOptions);
    }
    return key.toString();
  }

  private String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }
}
