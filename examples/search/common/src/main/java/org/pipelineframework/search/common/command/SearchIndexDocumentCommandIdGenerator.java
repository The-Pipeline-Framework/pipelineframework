package org.pipelineframework.search.common.command;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.dto.SearchIndexDocumentDto;
import org.pipelineframework.search.common.util.HashingUtils;

@ApplicationScoped
public class SearchIndexDocumentCommandIdGenerator implements CommandIdGenerator<Object> {
  @Override
  public String commandId(CommandDescriptor descriptor, Object input) {
    if (descriptor == null) {
      throw new IllegalArgumentException("descriptor is required");
    }
    SearchIndexIdentity identity = identity(input);
    if (identity.docId() == null) {
      throw new IllegalArgumentException("docId is required");
    }
    if (identity.batchIndex() == null || identity.batchIndex() < 0) {
      throw new IllegalArgumentException("batchIndex must be >= 0");
    }
    if (identity.vectorVersion() == null || identity.vectorVersion().isBlank()) {
      throw new IllegalArgumentException("vectorVersion is required");
    }
    if (identity.vectorHash() == null || identity.vectorHash().isBlank()) {
      throw new IllegalArgumentException("vectorHash is required");
    }
    return descriptor.command() + ":" + HashingUtils.sha256Base64Url(String.join("|",
        identity.docId().toString(),
        identity.batchIndex().toString(),
        identity.vectorVersion().trim(),
        identity.vectorHash().trim()));
  }

  private SearchIndexIdentity identity(Object input) {
    if (input instanceof SearchIndexDocument document) {
      return new SearchIndexIdentity(document.docId, document.batchIndex, document.vectorVersion, document.vectorHash);
    }
    if (input instanceof SearchIndexDocumentDto dto) {
      return new SearchIndexIdentity(dto.getDocId(), dto.getBatchIndex(), dto.getVectorVersion(), dto.getVectorHash());
    }
    if (input == null) {
      throw new IllegalArgumentException("search index document is required");
    }
    throw new IllegalArgumentException("unsupported search index document type: " + input.getClass().getName());
  }

  private record SearchIndexIdentity(
      java.util.UUID docId,
      Integer batchIndex,
      String vectorVersion,
      String vectorHash) {
  }
}
