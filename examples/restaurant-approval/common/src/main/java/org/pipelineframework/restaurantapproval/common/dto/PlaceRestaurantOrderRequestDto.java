package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.UUID;
import lombok.*;

import java.math.BigDecimal;

@Value
@Builder
@JsonDeserialize(builder = PlaceRestaurantOrderRequestDto.PlaceRestaurantOrderRequestDtoBuilder.class)
public class PlaceRestaurantOrderRequestDto {
  UUID id;
  UUID requestId;
  String customerName;
  String restaurantName;
  String items;
  BigDecimal totalAmount;
  String currency;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class PlaceRestaurantOrderRequestDtoBuilder {}
}