package org.pipelineframework.search.common.dto;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = IndexAckDto.IndexAckDtoBuilder.class)
public class IndexAckDto {
  UUID docId;
  String indexVersion;
  String tokensHash;
  Instant indexedAt;
  Boolean success;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class IndexAckDtoBuilder {}
}
