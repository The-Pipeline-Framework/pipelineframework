package org.pipelineframework.search.common.dto;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(builder = ParsedDocumentDto.ParsedDocumentDtoBuilder.class)
public class ParsedDocumentDto {
  UUID id;
  UUID docId;
  String title;
  String content;
  String contentHash;
  Instant extractedAt;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class ParsedDocumentDtoBuilder {}
}
