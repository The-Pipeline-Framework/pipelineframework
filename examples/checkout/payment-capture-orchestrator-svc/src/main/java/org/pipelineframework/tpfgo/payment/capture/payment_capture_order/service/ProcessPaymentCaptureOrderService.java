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
import org.pipelineframework.tpfgo.common.domain.PaymentCaptureResult;

@PipelineStep(
    inputType = OrderDelivered.class,
    outputType = PaymentCaptureResult.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service.OrderDeliveredMapper.class,
    outboundMapper = org.pipelineframework.tpfgo.payment.capture.payment_capture_order.service.PaymentCaptureResultMapper.class
)
@ApplicationScoped
public class ProcessPaymentCaptureOrderService implements ReactiveService<OrderDelivered, PaymentCaptureResult> {

    private final Clock clock;
    private final Supplier<UUID> uuidSupplier;

    @Inject
    public ProcessPaymentCaptureOrderService(Clock clock, Supplier<UUID> uuidSupplier) {
        this.clock = clock;
        this.uuidSupplier = uuidSupplier;
    }

    @Override
    public Uni<PaymentCaptureResult> process(OrderDelivered input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        Instant now = Instant.now(clock);
        if (input.amount().signum() <= 0) {
            return Uni.createFrom().item(new PaymentCaptureResult(
                input.orderId(),
                null,
                now,
                input.amount(),
                input.currency(),
                "FAILED",
                "PAYMENT_CAPTURE_REJECTED",
                "amount must be positive"));
        }
        return Uni.createFrom().item(new PaymentCaptureResult(
            input.orderId(),
            uuidSupplier.get(),
            now,
            input.amount(),
            input.currency(),
            "CAPTURED",
            null,
            null));
    }
}
