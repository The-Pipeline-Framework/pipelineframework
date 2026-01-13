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
public class RawDocument extends BaseEntity implements Serializable {

  public String sourceUrl;
  public String fetchOptions;
  public String rawContent;
  public String rawContentHash;
  public Instant fetchedAt;

}
