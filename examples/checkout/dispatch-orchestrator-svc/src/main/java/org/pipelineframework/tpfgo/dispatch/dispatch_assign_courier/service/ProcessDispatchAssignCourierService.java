package org.pipelineframework.tpfgo.dispatch.dispatch_assign_courier.service;

import java.time.Clock;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.DeliveryAssigned;
import org.pipelineframework.tpfgo.common.domain.OrderReadyForDispatch;

@PipelineStep
@ApplicationScoped
public class ProcessDispatchAssignCourierService implements ReactiveService<OrderReadyForDispatch, DeliveryAssigned> {

    private final Clock clock;
    private final Supplier<UUID> uuidSupplier;

    @Inject
    public ProcessDispatchAssignCourierService(Clock clock, Supplier<UUID> uuidSupplier) {
        this.clock = clock;
        this.uuidSupplier = uuidSupplier;
    }

    @Override
    public Uni<DeliveryAssigned> process(OrderReadyForDispatch input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        return Uni.createFrom().item(new DeliveryAssigned(
            input.orderId(),
            input.customerId(),
            input.restaurantId(),
            input.totalAmount(),
            input.currency(),
            input.kitchenTicketId(),
            uuidSupplier.get(),
            uuidSupplier.get(),
            18,
            Instant.now(clock),
            input.lineageDigest()));
    }
}
