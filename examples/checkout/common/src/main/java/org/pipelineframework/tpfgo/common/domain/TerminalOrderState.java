package org.pipelineframework.tpfgo.common.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record TerminalOrderState(
    UUID orderId,
    String outcome,
    Instant resolvedAt,
    String resolutionAction,
    String paymentStatus,
    UUID paymentId,
    String failureCode
) {
    public TerminalOrderState {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        Objects.requireNonNull(resolvedAt, "resolvedAt must not be null");
        Objects.requireNonNull(resolutionAction, "resolutionAction must not be null");
        Objects.requireNonNull(paymentStatus, "paymentStatus must not be null");
    }
}
