package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import jakarta.persistence.Entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class ParsedDocument extends BaseEntity implements Serializable {

  public UUID docId;
  public String title;
  public String content;
  public String contentHash;
  public Instant extractedAt;

}
