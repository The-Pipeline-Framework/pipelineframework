package org.pipelineframework.restaurantapproval.finalize_restaurant_decision.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantDecision;
import org.pipelineframework.restaurantapproval.common.domain.TerminalOrderState;
import org.pipelineframework.service.ReactiveService;

@PipelineStep
@ApplicationScoped
public class ProcessFinalizeRestaurantDecisionService
    implements ReactiveService<RestaurantDecision, TerminalOrderState> {

  @Override
  public Uni<TerminalOrderState> process(RestaurantDecision input) {
    return Uni.createFrom().item(() -> Objects.requireNonNull(input, "input must not be null")
        .toTerminalOrderState());
  }
}
