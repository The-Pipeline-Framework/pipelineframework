package org.pipelineframework.checkout.deliverorder.order_delivered.service;

import org.pipelineframework.checkout.deliverorder.common.domain.DispatchedOrder;
import org.pipelineframework.checkout.deliverorder.common.domain.DeliveredOrder;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

@PipelineStep(
    inputType = DispatchedOrder.class,
    outputType = DeliveredOrder.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.checkout.deliverorder.common.mapper.DispatchedOrderMapper.class,
    outboundMapper = org.pipelineframework.checkout.deliverorder.common.mapper.DeliveredOrderMapper.class
)
@ApplicationScoped
public class ProcessOrderDeliveredService
    implements ReactiveService<DispatchedOrder, DeliveredOrder> {

  private final Clock clock;

  @Inject
  public ProcessOrderDeliveredService(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public Uni<DeliveredOrder> process(DispatchedOrder input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
    }
    DeliveredOrder output = new DeliveredOrder(
        input.orderId(),
        input.customerId(),
        input.readyAt(),
        input.dispatchId(),
        input.dispatchedAt(),
        Instant.now(clock));
    return Uni.createFrom().item(output);
  }
}
