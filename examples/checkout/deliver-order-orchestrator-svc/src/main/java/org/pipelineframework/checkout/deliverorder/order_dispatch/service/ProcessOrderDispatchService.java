package org.pipelineframework.checkout.deliverorder.order_dispatch.service;

import org.pipelineframework.checkout.deliverorder.common.domain.ReadyOrder;
import org.pipelineframework.checkout.deliverorder.common.domain.DispatchedOrder;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.UUID;
import org.jboss.logging.Logger;

@PipelineStep(
    inputType = ReadyOrder.class,
    outputType = DispatchedOrder.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.checkout.deliverorder.common.mapper.ReadyOrderMapper.class,
    outboundMapper = org.pipelineframework.checkout.deliverorder.common.mapper.DispatchedOrderMapper.class
)
@ApplicationScoped
public class ProcessOrderDispatchService
    implements ReactiveService<ReadyOrder, DispatchedOrder> {

  private static final Logger LOG = Logger.getLogger(ProcessOrderDispatchService.class);

  private final Clock clock;
  private final Supplier<UUID> uuidSupplier;

  @Inject
  public ProcessOrderDispatchService(Clock clock, Supplier<UUID> uuidSupplier) {
    this.clock = clock;
    this.uuidSupplier = uuidSupplier;
  }

  @Override
  public Uni<DispatchedOrder> process(ReadyOrder input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
    }
    DispatchedOrder output = new DispatchedOrder(
        input.orderId(),
        input.customerId(),
        input.readyAt(),
        uuidSupplier.get(),
        Instant.now(clock));
    LOG.infof("dispatch created orderId=%s dispatchId=%s", output.orderId(), output.dispatchId());
    return Uni.createFrom().item(output);
  }
}
