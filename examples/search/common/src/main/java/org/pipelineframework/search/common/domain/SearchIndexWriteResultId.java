package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class SearchIndexWriteResultId implements Serializable {
  public UUID docId;
  public String externalId;

  public SearchIndexWriteResultId() {
  }

  public SearchIndexWriteResultId(UUID docId, String externalId) {
    this.docId = docId;
    this.externalId = externalId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SearchIndexWriteResultId that)) {
      return false;
    }
    return Objects.equals(docId, that.docId) && Objects.equals(externalId, that.externalId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, externalId);
  }
}
