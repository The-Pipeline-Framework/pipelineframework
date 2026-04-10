package org.pipelineframework.tpfgo.restaurant.acceptance.restaurant_accept_order.service;

import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.OrderAcceptedByRestaurant;
import org.pipelineframework.tpfgo.common.domain.OrderApproved;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

@PipelineStep(
    inputType = OrderApproved.class,
    outputType = OrderAcceptedByRestaurant.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.restaurant.acceptance.restaurant_accept_order.service.OrderApprovedMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.restaurant.acceptance.restaurant_accept_order.service.OrderAcceptedByRestaurantMapper.class
)
@ApplicationScoped
public class ProcessRestaurantAcceptOrderService
    implements ReactiveService<OrderApproved, OrderAcceptedByRestaurant> {

    private final Clock clock;

    @Inject
    public ProcessRestaurantAcceptOrderService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<OrderAcceptedByRestaurant> process(OrderApproved input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return Uni.createFrom().item(new OrderAcceptedByRestaurant(
            input.orderId(),
            input.requestId(),
            input.customerId(),
            input.restaurantId(),
            input.totalAmount(),
            input.currency(),
            Instant.now(clock),
            DeterministicIds.uuid("kitchen-ticket", input.orderId().toString())));
    }
}
