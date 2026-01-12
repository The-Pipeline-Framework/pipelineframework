package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import jakarta.persistence.Entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.pipelineframework.cache.CacheKey;

@Setter
@Getter
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class TokenBatch extends BaseEntity implements Serializable, CacheKey {

  public UUID docId;
  public String tokens;
  public String tokensHash;
  public Instant tokenizedAt;

  @Override
  public String cacheKey() {
    if (docId != null) {
      return docId.toString();
    }
    if (id != null) {
      return id.toString();
    }
    return "missing-doc-id";
  }
}
