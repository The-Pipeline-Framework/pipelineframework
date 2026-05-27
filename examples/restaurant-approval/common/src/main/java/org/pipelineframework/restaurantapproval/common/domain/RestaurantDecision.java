package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.util.UUID;

@JsonSerialize(using = RestaurantDecisionJsonSerializer.class)
@JsonDeserialize(using = RestaurantDecisionJsonDeserializer.class)
public sealed interface RestaurantDecision extends Serializable
    permits RestaurantOrderAccepted, RestaurantOrderDeclined {

  UUID orderId();

  <R> R accept(RestaurantDecisionVisitor<R> visitor);

  TerminalOrderState toTerminalOrderState();
}
