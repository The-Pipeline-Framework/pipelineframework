package org.pipelineframework.search.common.command;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.search.common.domain.SearchIndexDocument;
import org.pipelineframework.search.common.util.HashingUtils;

@ApplicationScoped
public class SearchIndexDocumentCommandIdGenerator implements CommandIdGenerator<SearchIndexDocument> {
  @Override
  public String commandId(CommandDescriptor descriptor, SearchIndexDocument input) {
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

  private SearchIndexIdentity identity(SearchIndexDocument input) {
    if (input == null) {
      throw new IllegalArgumentException("search index document is required");
    }
    return new SearchIndexIdentity(input.docId, input.batchIndex, input.vectorVersion, input.vectorHash);
  }

  private record SearchIndexIdentity(
      java.util.UUID docId,
      Integer batchIndex,
      String vectorVersion,
      String vectorHash) {
  }
}
