package org.pipelineframework.restaurantapproval.finalize_restaurant_decision.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.rest.RestReactiveServiceAdapter;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantDecision;
import org.pipelineframework.restaurantapproval.common.domain.TerminalOrderState;
import org.pipelineframework.restaurantapproval.common.mapper.RestaurantDecisionMapper;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = org.pipelineframework.restaurantapproval.common.domain.RestaurantDecision.class,
    outputType = org.pipelineframework.restaurantapproval.common.domain.TerminalOrderState.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.rest.RestReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.restaurantapproval.common.mapper.RestaurantDecisionMapper.class,
    outboundMapper = org.pipelineframework.restaurantapproval.common.mapper.TerminalOrderStateMapper.class,
    runOnVirtualThreads = false
)
@ApplicationScoped
public class ProcessFinalizeRestaurantDecisionService
    implements ReactiveService<RestaurantDecision, TerminalOrderState> {

  @Override
  public Uni<TerminalOrderState> process(RestaurantDecision input) {
    return Uni.createFrom().item(() -> Objects.requireNonNull(input, "input must not be null")
        .toTerminalOrderState());
  }
}
