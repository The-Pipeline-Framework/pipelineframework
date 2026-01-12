package org.pipelineframework.search.common.dto;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = RawDocumentDto.RawDocumentDtoBuilder.class)
public class RawDocumentDto {
  UUID id;
  UUID docId;
  String sourceUrl;
  String rawContent;
  String rawContentHash;
  Instant fetchedAt;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class RawDocumentDtoBuilder {}
}
