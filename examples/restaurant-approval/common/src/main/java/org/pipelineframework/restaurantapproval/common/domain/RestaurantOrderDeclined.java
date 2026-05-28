package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("declined")
public record RestaurantOrderDeclined(
    UUID orderId,
    Instant decidedAt,
    String note,
    String declineReason
) implements RestaurantDecision {

  public RestaurantOrderDeclined {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
    declineReason = Objects.requireNonNullElse(declineReason, "no reason provided").trim();
    if (declineReason.isBlank()) {
      declineReason = "no reason provided";
    }
  }

  @Override
  public <R> R accept(RestaurantDecisionVisitor<R> visitor) {
    return visitor.declined(this);
  }

  @Override
  public TerminalOrderState toTerminalOrderState() {
    TerminalOrderState state = new TerminalOrderState();
    state.orderId = orderId;
    state.outcome = "DECLINED";
    state.resolvedAt = decidedAt;
    state.restaurantStatus = "DECLINED";
    state.summary = note.isBlank() ? declineReason : note + " (" + declineReason + ")";
    return state;
  }
}
