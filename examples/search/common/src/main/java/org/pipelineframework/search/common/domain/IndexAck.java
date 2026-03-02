package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.time.Instant;
import jakarta.persistence.Entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@Entity
@NoArgsConstructor
public class IndexAck extends BaseEntity implements Serializable {

  private String indexVersion;
  private String tokensHash;
  private Integer tokenBatchCount;
  private Integer uniqueTokenCount;
  private String topToken;
  private Instant indexedAt;
  private Boolean success;

}
