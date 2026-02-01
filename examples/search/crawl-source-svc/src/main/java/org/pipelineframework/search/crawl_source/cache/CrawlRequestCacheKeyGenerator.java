package org.pipelineframework.search.crawl_source.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.util.FetchOptionsNormalizer;

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

  /**
   * Builds the base cache key from method parameters, preferring a CrawlRequest's source URL and fetch options when present.
   *
   * @param methodParams the method's invocation parameters; if empty returns "no-params". If the first parameter is a CrawlRequest with a non-blank sourceUrl, that sourceUrl and any normalized fetch options are used to form the key; otherwise a generic key is derived from all parameters.
   * @return `no-params` when no parameters; otherwise a base key string. When derived from a CrawlRequest the format is "<fully-qualified RawDocument class name>:<normalizedSourceUrl>" optionally followed by "|<fetchOptions>".
   */
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
    key.append(RawDocument.class.getName()).append(":").append(sourceUrl);
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