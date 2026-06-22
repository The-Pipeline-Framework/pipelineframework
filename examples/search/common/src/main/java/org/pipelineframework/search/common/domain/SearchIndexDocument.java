package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.time.Instant;
import jakarta.persistence.Column;
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
@IdClass(SearchIndexDocumentId.class)
@NoArgsConstructor
public class SearchIndexDocument extends BaseEntity implements Serializable {
  @Id
  @Column(nullable = false)
  public String externalId;
  public Integer batchIndex;
  public Integer tokenCount;
  public String tokens;
  public String tokensHash;
  public String contentHash;
  public String vectorHash;
  public String vectorVersion;
  public String indexName;
  public Instant preparedAt;

  @PrePersist
  protected void validateRequiredFields() {
    if (docId == null) {
      throw new IllegalStateException("docId is required");
    }
    if (externalId == null || externalId.isBlank()) {
      throw new IllegalStateException("externalId is required");
    }
    if (batchIndex == null || batchIndex < 0) {
      throw new IllegalStateException("batchIndex must be >= 0");
    }
    if (tokensHash == null || tokensHash.isBlank()) {
      throw new IllegalStateException("tokensHash is required");
    }
    if (vectorHash == null || vectorHash.isBlank()) {
      throw new IllegalStateException("vectorHash is required");
    }
    if (vectorVersion == null || vectorVersion.isBlank()) {
      throw new IllegalStateException("vectorVersion is required");
    }
    if (indexName == null || indexName.isBlank()) {
      throw new IllegalStateException("indexName is required");
    }
  }
}
