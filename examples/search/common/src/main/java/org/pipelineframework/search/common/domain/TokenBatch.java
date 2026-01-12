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
public class TokenBatch extends BaseEntity implements Serializable {

  public String tokens;
  public String tokensHash;
  public Instant tokenizedAt;

}
