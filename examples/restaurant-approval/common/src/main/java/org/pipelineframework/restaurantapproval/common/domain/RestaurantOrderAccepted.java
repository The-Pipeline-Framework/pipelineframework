package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("accepted")
public record RestaurantOrderAccepted(
    UUID orderId,
    Instant decidedAt,
    String note
) implements RestaurantDecision {

  public RestaurantOrderAccepted {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
  }

  @Override
  public <R> R accept(RestaurantDecisionVisitor<R> visitor) {
    return visitor.accepted(this);
  }

  @Override
  public TerminalOrderState toTerminalOrderState() {
    TerminalOrderState state = new TerminalOrderState();
    state.orderId = orderId;
    state.outcome = "APPROVED";
    state.resolvedAt = decidedAt;
    state.restaurantStatus = "ACCEPTED";
    state.summary = note.isBlank() ? "Restaurant approved the order." : note;
    return state;
  }
}
