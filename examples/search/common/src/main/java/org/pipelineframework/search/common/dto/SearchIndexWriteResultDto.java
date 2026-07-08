package org.pipelineframework.search.common.dto;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = SearchIndexWriteResultDto.SearchIndexWriteResultDtoBuilder.class)
public class SearchIndexWriteResultDto {
  UUID docId;
  String externalId;
  String commandId;
  String indexName;
  String resultStatus;
  Boolean createdOrUpdated;
  Boolean recordedDuplicate;
  Integer batchIndex;
  Integer tokenCount;
  String tokens;
  String tokensHash;
  String vectorHash;
  String vectorVersion;
  Instant indexedAt;

  @JsonPOJOBuilder(withPrefix = "")
  public static class SearchIndexWriteResultDtoBuilder {}
}
