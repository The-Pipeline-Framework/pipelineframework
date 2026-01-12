package org.pipelineframework.search.common.dto;

import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.Value;


@Value
@Builder
@JsonDeserialize(builder = CrawlRequestDto.CrawlRequestDtoBuilder.class)
public class CrawlRequestDto {
  UUID docId;
  String sourceUrl;
  String fetchMethod;
  String accept;
  String acceptLanguage;
  String authScope;
  Map<String, String> fetchHeaders;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class CrawlRequestDtoBuilder {}
}
