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
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class
)
@ApplicationScoped
public class ProcessOrderDeliveredService
    implements ReactiveService<DispatchedOrder, DeliveredOrder> {

  private final Clock clock;

  /**
   * Creates a ProcessOrderDeliveredService that uses the provided clock to obtain current instants for delivered orders.
   *
   * @param clock the clock used to produce timestamps for delivered orders
   * @throws NullPointerException if {@code clock} is {@code null}
   */
  @Inject
  public ProcessOrderDeliveredService(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  /**
   * Builds a DeliveredOrder from a DispatchedOrder, setting the delivered timestamp to the current instant from the injected clock.
   *
   * If {@code input} is null the method yields a failed Uni containing an {@link IllegalArgumentException}.
   *
   * @param input the dispatched order to convert
   * @return the DeliveredOrder with its delivery time set to the current instant
   */
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