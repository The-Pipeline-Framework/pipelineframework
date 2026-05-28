package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("declined")
public record RestaurantOrderDeclinedDto(
    UUID orderId,
    Instant decidedAt,
    String note,
    String declineReason
) implements RestaurantDecisionDto {

  public RestaurantOrderDeclinedDto {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
    declineReason = Objects.requireNonNullElse(declineReason, "no reason provided").trim();
    if (declineReason.isBlank()) {
      declineReason = "no reason provided";
    }
  }
}
