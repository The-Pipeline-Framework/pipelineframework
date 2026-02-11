package org.pipelineframework.checkout.createorder.order_ready.service;

import org.pipelineframework.checkout.createorder.common.domain.InitialOrder;
import org.pipelineframework.checkout.createorder.common.domain.ReadyOrder;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

@PipelineStep(
    inputType = InitialOrder.class,
    outputType = ReadyOrder.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.checkout.createorder.common.mapper.InitialOrderMapper.class,
    outboundMapper = org.pipelineframework.checkout.createorder.common.mapper.ReadyOrderMapper.class
)
@ApplicationScoped
public class ProcessOrderReadyService
    implements ReactiveService<InitialOrder, ReadyOrder> {

  private final Clock clock;

  @Inject
  public ProcessOrderReadyService(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public Uni<ReadyOrder> process(InitialOrder input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("initialOrder must not be null"));
    }
    ReadyOrder output = new ReadyOrder(input.orderId(), input.customerId(), Instant.now(clock));
    return Uni.createFrom().item(output);
  }
}
