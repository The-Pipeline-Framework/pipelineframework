package org.pipelineframework.tpfgo.checkout.checkout_create_pending.service;

import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.checkout.checkout_validate_request.service.ValidatedOrderRequestMapper;
import org.pipelineframework.tpfgo.common.domain.OrderPending;
import org.pipelineframework.tpfgo.common.domain.ValidatedOrderRequest;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

@PipelineStep(
    inputType = ValidatedOrderRequest.class,
    outputType = OrderPending.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.checkout.checkout_validate_request.service.ValidatedOrderRequestMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.checkout.checkout_create_pending.service.OrderPendingMapper.class
)
@ApplicationScoped
public class ProcessCheckoutCreatePendingService implements ReactiveService<ValidatedOrderRequest, OrderPending> {

    private final Clock clock;

    @Inject
    public ProcessCheckoutCreatePendingService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<OrderPending> process(ValidatedOrderRequest input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return Uni.createFrom().item(new OrderPending(
            DeterministicIds.uuid(
                "order",
                input.requestId().toString(),
                input.customerId().toString(),
                input.restaurantId().toString()),
            input.requestId(),
            input.customerId(),
            input.restaurantId(),
            input.totalAmount(),
            input.currency(),
            Instant.now(clock)));
    }
}
