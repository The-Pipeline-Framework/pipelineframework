package org.pipelineframework.search.common.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.domain.SearchIndexWriteResult;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;
import org.pipelineframework.search.common.dto.SearchIndexWriteResultDto;

@ApplicationScoped
@Unremovable
public class SearchIndexWriteResultCacheKeyStrategy implements CacheKeyStrategy {

  @Override
  public Optional<String> resolveKey(Object item, PipelineContext context) {
    return identity(item).map(identity -> SearchIndexWriteResult.class.getName()
        + ":doc=" + identity.docId()
        + ":external=" + identity.externalId()
        + ":batch=" + identity.batchIndex()
        + ":tokens=" + identity.tokensHash()
        + ":vector=" + identity.vectorHash()
        + ":vectorVersion=" + identity.vectorVersion()
        + ":index=" + identity.indexName());
  }

  @Override
  public int priority() {
    return 66;
  }

  @Override
  public boolean supportsTarget(Class<?> targetType) {
    return targetType == SearchIndexWriteResult.class || targetType == SearchIndexWriteResultDto.class;
  }

  private Optional<Identity> identity(Object item) {
    if (item instanceof SearchIndexDocument document) {
      return identity(document.docId, document.externalId, document.batchIndex, document.tokensHash,
          document.vectorHash, document.vectorVersion, document.indexName);
    }
    if (item instanceof SearchIndexDocumentDto dto) {
      return identity(dto.getDocId(), dto.getExternalId(), dto.getBatchIndex(), dto.getTokensHash(),
          dto.getVectorHash(), dto.getVectorVersion(), dto.getIndexName());
    }
    if (item instanceof SearchIndexWriteResult result) {
      return identity(result.docId, result.externalId, result.batchIndex, result.tokensHash,
          result.vectorHash, result.vectorVersion, result.indexName);
    }
    if (item instanceof SearchIndexWriteResultDto dto) {
      return identity(dto.getDocId(), dto.getExternalId(), dto.getBatchIndex(), dto.getTokensHash(),
          dto.getVectorHash(), dto.getVectorVersion(), dto.getIndexName());
    }
    return Optional.empty();
  }

  private Optional<Identity> identity(
      Object docId,
      String externalId,
      Integer batchIndex,
      String tokensHash,
      String vectorHash,
      String vectorVersion,
      String indexName) {
    if (docId == null || isBlank(externalId) || batchIndex == null || batchIndex < 0
        || isBlank(tokensHash) || isBlank(vectorHash) || isBlank(vectorVersion) || isBlank(indexName)) {
      return Optional.empty();
    }
    return Optional.of(new Identity(
        docId.toString(), externalId.trim(), batchIndex, tokensHash.trim(), vectorHash.trim(),
        vectorVersion.trim(), indexName.trim()));
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private record Identity(
      String docId,
      String externalId,
      int batchIndex,
      String tokensHash,
      String vectorHash,
      String vectorVersion,
      String indexName) {
  }
}
