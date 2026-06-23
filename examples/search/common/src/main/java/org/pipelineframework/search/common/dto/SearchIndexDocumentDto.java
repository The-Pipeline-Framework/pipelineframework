package org.pipelineframework.search.common.dto;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = SearchIndexDocumentDto.SearchIndexDocumentDtoBuilder.class)
public class SearchIndexDocumentDto {
  UUID docId;
  String externalId;
  Integer batchIndex;
  Integer tokenCount;
  String tokens;
  String tokensHash;
  String contentHash;
  String vectorHash;
  String vectorVersion;
  String indexName;
  Instant preparedAt;

  @JsonPOJOBuilder(withPrefix = "")
  public static class SearchIndexDocumentDtoBuilder {}
}
