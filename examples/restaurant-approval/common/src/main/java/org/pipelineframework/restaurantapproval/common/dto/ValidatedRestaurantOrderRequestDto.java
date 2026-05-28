package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.UUID;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.Instant;
import java.time.Duration;
import java.time.Period;
import java.math.BigDecimal;

@Value
@Builder
@JsonDeserialize(builder = ValidatedRestaurantOrderRequestDto.ValidatedRestaurantOrderRequestDtoBuilder.class)
public class ValidatedRestaurantOrderRequestDto {
  UUID id;
  UUID requestId;
  String customerName;
  String restaurantName;
  String items;
  BigDecimal totalAmount;
  String currency;
  Instant validatedAt;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class ValidatedRestaurantOrderRequestDtoBuilder {}
}