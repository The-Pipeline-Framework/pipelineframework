package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("captured")
public record PaymentCaptured(
    UUID orderId,
    UUID paymentId,
    Instant processedAt,
    BigDecimal amount,
    String currency
) implements PaymentOutcome {
    public PaymentCaptured {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(processedAt, "processedAt must not be null");
        amount = CommonDomainValidation.requireNonNegative(amount, "amount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
    }

    @Override
    public String status() {
        return "CAPTURED";
    }

    @Override
    public TerminalOrderState toTerminalOrderState(Instant resolvedAt) {
        return new TerminalOrderState(orderId, "COMPLETED", resolvedAt, "none", status(), paymentId, null);
    }

    @Override
    public <R> R accept(PaymentOutcomeVisitor<R> visitor) {
        return visitor.captured(this);
    }
}
