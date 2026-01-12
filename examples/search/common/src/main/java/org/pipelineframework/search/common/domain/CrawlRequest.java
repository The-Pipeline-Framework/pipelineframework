package org.pipelineframework.search.common.domain;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import jakarta.persistence.Entity;
import jakarta.persistence.Transient;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Setter
@Getter
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class CrawlRequest extends BaseEntity implements Serializable {

  public UUID docId;
  public String sourceUrl;
  public String fetchMethod;
  public String accept;
  public String acceptLanguage;
  public String authScope;
  @Transient
  public Map<String, String> fetchHeaders;

}
