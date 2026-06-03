package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public class EmbeddedChunkId implements Serializable {

  private static final long serialVersionUID = 1L;

  private UUID docId;
  private Integer batchIndex;
  private String vectorHash;

  public EmbeddedChunkId() {
  }

  public EmbeddedChunkId(UUID docId, Integer batchIndex, String vectorHash) {
    this.docId = docId;
    this.batchIndex = batchIndex;
    this.vectorHash = vectorHash;
  }

  public UUID getDocId() {
    return docId;
  }

  public void setDocId(UUID docId) {
    this.docId = docId;
  }

  public Integer getBatchIndex() {
    return batchIndex;
  }

  public void setBatchIndex(Integer batchIndex) {
    this.batchIndex = batchIndex;
  }

  public String getVectorHash() {
    return vectorHash;
  }

  public void setVectorHash(String vectorHash) {
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
