package org.pipelineframework.search.embed_content.cache;

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.cache.PipelineCacheKeyFormat;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.TokenBatch;

@ApplicationScoped
@Unremovable
public class EmbedContentCacheKeyGenerator implements CacheKeyGenerator {

  private final String vectorVersion;

  @Inject
  public EmbedContentCacheKeyGenerator(
      @ConfigProperty(name = "search.embed.vector-version", defaultValue = "v1") String vectorVersion) {
    this.vectorVersion = normalizeVectorVersion(vectorVersion);
  }

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
    return EmbeddedChunk.class.getName()
        + ":doc=" + docId
        + ":batch=" + batch.batchIndex
        + ":tokens=" + tokensHash
        + ":vector=" + vectorVersion;
  }

  private String resolveVectorVersion() {
    return vectorVersion;
  }

  private String normalizeVectorVersion(String configured) {
    return configured == null || configured.isBlank() ? "v1" : configured.trim();
  }

  private String normalize(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }
}
