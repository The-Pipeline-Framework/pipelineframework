package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class TokenBatchId implements Serializable {

  private static final long serialVersionUID = 1L;

  public UUID docId;
  public Integer batchIndex;

  public TokenBatchId() {
  }

  public TokenBatchId(UUID docId, Integer batchIndex) {
    this.docId = docId;
    this.batchIndex = batchIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TokenBatchId that)) {
      return false;
    }
    return Objects.equals(docId, that.docId) && Objects.equals(batchIndex, that.batchIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, batchIndex);
  }
}
