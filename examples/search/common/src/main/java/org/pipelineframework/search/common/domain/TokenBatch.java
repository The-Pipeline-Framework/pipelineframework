package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.time.Instant;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.PrePersist;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@Entity
@IdClass(TokenBatchId.class)
@NoArgsConstructor
public class TokenBatch extends BaseEntity implements Serializable {

  @Id
  public Integer batchIndex;
  public String tokens;
  public String tokensHash;
  public String contentHash;
  public Instant tokenizedAt;

  @PrePersist
  protected void ensureBatchIndex() {
    if (batchIndex == null) {
      throw new IllegalStateException("batchIndex is required");
    }
  }
}
