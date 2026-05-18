package org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.OrderDelivered;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptured;
import org.pipelineframework.tpfgo.common.domain.PaymentOutcome;
import org.pipelineframework.tpfgo.common.domain.PaymentRejected;

@PipelineStep
@ApplicationScoped
public class ProcessPaymentCaptureOrderService implements ReactiveService<OrderDelivered, PaymentOutcome> {

    private final Clock clock;
    private final Supplier<UUID> uuidSupplier;

    @Inject
    public ProcessPaymentCaptureOrderService(Clock clock, Supplier<UUID> uuidSupplier) {
        this.clock = clock;
        this.uuidSupplier = uuidSupplier;
    }

    @Override
    public Uni<PaymentOutcome> process(OrderDelivered input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        Instant now = Instant.now(clock);
        if (input.amount().signum() <= 0) {
            return Uni.createFrom().item(new PaymentRejected(
                input.orderId(),
                now,
                input.amount(),
                input.currency(),
                "PAYMENT_CAPTURE_REJECTED",
                "amount must be positive"));
        }
        return Uni.createFrom().item(new PaymentCaptured(
            input.orderId(),
            uuidSupplier.get(),
            now,
            input.amount(),
            input.currency()));
    }
}
