package org.pipelineframework.tpfgo.consumer.validation.consumer_validate_order.service;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.OrderApproved;
import org.pipelineframework.tpfgo.common.domain.OrderPending;

@PipelineStep
@ApplicationScoped
public class ProcessConsumerValidateOrderService implements ReactiveService<OrderPending, OrderApproved> {

    private final Clock clock;

    @Inject
    public ProcessConsumerValidateOrderService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<OrderApproved> process(OrderPending input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        String riskBand = input.totalAmount().compareTo(new BigDecimal("100.00")) > 0 ? "HIGH" : "LOW";
        return Uni.createFrom().item(new OrderApproved(
            input.orderId(),
            input.requestId(),
            input.customerId(),
            input.restaurantId(),
            input.totalAmount(),
            input.currency(),
            Instant.now(clock),
            riskBand));
    }
}
