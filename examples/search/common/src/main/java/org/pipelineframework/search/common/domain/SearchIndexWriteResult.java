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
@IdClass(SearchIndexWriteResultId.class)
@NoArgsConstructor
public class SearchIndexWriteResult extends BaseEntity implements Serializable {
  @Id
  @Column(nullable = false)
  public String externalId;
  public String commandId;
  public String indexName;
  public String resultStatus;
  public Boolean createdOrUpdated;
  public Boolean recordedDuplicate;
  public Integer batchIndex;
  public Integer tokenCount;
  public String tokens;
  public String tokensHash;
  public String vectorHash;
  public String vectorVersion;
  public Instant indexedAt;

  @PrePersist
  protected void validateRequiredFields() {
    if (docId == null) {
      throw new IllegalStateException("docId is required");
    }
    if (externalId == null || externalId.isBlank()) {
      throw new IllegalStateException("externalId is required");
    }
    if (commandId == null || commandId.isBlank()) {
      throw new IllegalStateException("commandId is required");
    }
    if (indexName == null || indexName.isBlank()) {
      throw new IllegalStateException("indexName is required");
    }
  }
}
