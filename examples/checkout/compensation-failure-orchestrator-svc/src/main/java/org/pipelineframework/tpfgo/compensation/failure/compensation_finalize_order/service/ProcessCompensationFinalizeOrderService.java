package org.pipelineframework.tpfgo.compensation.failure.compensation_finalize_order.service;

import java.time.Clock;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.tpfgo.common.domain.PaymentCaptureResult;
import org.pipelineframework.tpfgo.common.domain.TerminalOrderState;

@PipelineStep
@ApplicationScoped
public class ProcessCompensationFinalizeOrderService
    implements ReactiveService<PaymentCaptureResult, TerminalOrderState> {

    private final Clock clock;

    @Inject
    public ProcessCompensationFinalizeOrderService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Uni<TerminalOrderState> process(PaymentCaptureResult input) {
        if (input == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
        }
        Instant now = Instant.now(clock);
        if ("CAPTURED".equals(input.status())) {
            return Uni.createFrom().item(new TerminalOrderState(
                input.orderId(),
                "COMPLETED",
                now,
                "none",
                input.status(),
                input.paymentId(),
                null));
        }
        return Uni.createFrom().item(new TerminalOrderState(
            input.orderId(),
            "FAILED_COMPENSATED",
            now,
            "manual-review",
            input.status(),
            input.paymentId(),
            input.failureCode()));
    }
}
