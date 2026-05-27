package org.pipelineframework.restaurantapproval.create_pending_approval.service;

import org.pipelineframework.restaurantapproval.common.domain.ValidatedRestaurantOrderRequest;
import org.pipelineframework.restaurantapproval.common.domain.PendingRestaurantApproval;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.rest.RestReactiveServiceAdapter;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;

@PipelineStep(
    inputType = org.pipelineframework.restaurantapproval.common.domain.ValidatedRestaurantOrderRequest.class,
    outputType = org.pipelineframework.restaurantapproval.common.domain.PendingRestaurantApproval.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.rest.RestReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.restaurantapproval.common.mapper.ValidatedRestaurantOrderRequestMapper.class,
    outboundMapper = org.pipelineframework.restaurantapproval.common.mapper.PendingRestaurantApprovalMapper.class,
    runOnVirtualThreads = false
)
@ApplicationScoped
public class ProcessCreatePendingApprovalService
    implements ReactiveService<ValidatedRestaurantOrderRequest, PendingRestaurantApproval> {

  private final Clock clock = Clock.systemUTC();

  @Override
  public Uni<PendingRestaurantApproval> process(ValidatedRestaurantOrderRequest input) {
    return Uni.createFrom().item(() -> {
      Objects.requireNonNull(input, "input must not be null");
      PendingRestaurantApproval output = new PendingRestaurantApproval();
      output.orderId = UUID.randomUUID();
      output.requestId = input.requestId;
      output.customerName = input.customerName;
      output.restaurantName = input.restaurantName;
      output.items = input.items != null ? new ArrayList<>(input.items) : null;
      output.totalAmount = input.totalAmount;
      output.currency = input.currency;
      output.createdAt = Instant.now(clock);
      return output;
    });
  }
}
