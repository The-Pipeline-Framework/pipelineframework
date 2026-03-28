package org.pipelineframework.tpfgo.delivery.execution.delivery_execute_order.service;

import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.DeliveryAssigned;
import org.pipelineframework.tpfgo.common.domain.OrderDelivered;

@PipelineStep(
    inputType = DeliveryAssigned.class,
    outputType = OrderDelivered.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.delivery.execution.delivery_execute_order.service.DeliveryAssignedMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.delivery.execution.delivery_execute_order.service.OrderDeliveredMapper.class
)
@ApplicationScoped
public class ProcessDeliveryExecuteOrderService implements ReactiveService<DeliveryAssigned, OrderDelivered> {

    private final Clock clock;

    @Inject
    public ProcessDeliveryExecuteOrderService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<OrderDelivered> process(DeliveryAssigned input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return Uni.createFrom().item(new OrderDelivered(
            input.orderId(),
            input.customerId(),
            input.dispatchId(),
            input.courierId(),
            input.restaurantId(),
            input.kitchenTicketId(),
            Instant.now(clock),
            input.totalAmount(),
            input.currency(),
            input.lineageDigest()));
    }
}
