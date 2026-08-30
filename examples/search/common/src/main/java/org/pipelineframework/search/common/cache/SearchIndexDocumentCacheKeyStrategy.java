package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;

@ApplicationScoped
@Unremovable
public class SearchIndexDocumentCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    return identity(item).map(identity -> SearchIndexDocument.class.getName()
        + ":doc=" + identity.docId()
        + ":batch=" + identity.batchIndex()
        + ":tokens=" + identity.tokensHash()
        + ":vector=" + identity.vectorHash()
        + ":vectorVersion=" + identity.vectorVersion()
        + ":index=" + identity.indexName());
  }

  @Override
  public int priority() {
    return 65;
  }

  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == SearchIndexDocument.class || targetType == SearchIndexDocumentDto.class;
  }

  private Optional<Identity> identity(Object item) {
    if (item instanceof EmbeddedChunk chunk) {
      return identity(chunk.docId, chunk.batchIndex, chunk.tokensHash, chunk.vectorHash,
          chunk.vectorVersion, configuredIndexName());
    }
    if (item instanceof EmbeddedChunkDto dto) {
      return identity(dto.getDocId(), dto.getBatchIndex(), dto.getTokensHash(), dto.getVectorHash(),
          dto.getVectorVersion(), configuredIndexName());
    }
    if (item instanceof SearchIndexDocument document) {
      return identity(document.docId, document.batchIndex, document.tokensHash, document.vectorHash,
          document.vectorVersion, document.indexName);
    }
    if (item instanceof SearchIndexDocumentDto dto) {
      return identity(dto.getDocId(), dto.getBatchIndex(), dto.getTokensHash(), dto.getVectorHash(),
          dto.getVectorVersion(), dto.getIndexName());
    }
    return Optional.empty();
  }

  private Optional<Identity> identity(
      Object docId,
      Integer batchIndex,
      String tokensHash,
      String vectorHash,
      String vectorVersion,
      String indexName) {
    if (docId == null || batchIndex == null || batchIndex < 0
        || isBlank(tokensHash) || isBlank(vectorHash) || isBlank(vectorVersion) || isBlank(indexName)) {
      return Optional.empty();
    }
    return Optional.of(new Identity(
        docId.toString(), batchIndex, tokensHash.trim(), vectorHash.trim(), vectorVersion.trim(), indexName.trim()));
  }

  private String configuredIndexName() {
    try {
      return ConfigProvider.getConfig()
          .getOptionalValue("search.index.opensearch.index", String.class)
          .filter(value -> !value.isBlank())
          .map(String::trim)
          .orElse("search-documents");
    } catch (IllegalStateException ignored) {
      return "search-documents";
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private record Identity(
      String docId,
      int batchIndex,
      String tokensHash,
      String vectorHash,
      String vectorVersion,
      String indexName) {
  }
}
