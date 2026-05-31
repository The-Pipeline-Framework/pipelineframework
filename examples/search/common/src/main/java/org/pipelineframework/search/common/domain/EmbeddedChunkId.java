package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class EmbeddedChunkId implements Serializable {

  private static final long serialVersionUID = 1L;

  public UUID docId;
  public Integer batchIndex;
  public String vectorHash;

  public EmbeddedChunkId() {
  }

  public EmbeddedChunkId(UUID docId, Integer batchIndex, String vectorHash) {
    this.docId = docId;
    this.batchIndex = batchIndex;
    this.vectorHash = vectorHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EmbeddedChunkId that)) {
      return false;
    }
    return Objects.equals(docId, that.docId)
        && Objects.equals(batchIndex, that.batchIndex)
        && Objects.equals(vectorHash, that.vectorHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, batchIndex, vectorHash);
  }
}
