package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record PaymentCaptureResult(
    UUID orderId,
    UUID paymentId,
    Instant processedAt,
    BigDecimal amount,
    String currency,
    String status,
    String failureCode,
    String failureReason
) {
    public PaymentCaptureResult {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(processedAt, "processedAt must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }
}
