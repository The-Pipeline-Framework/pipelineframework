package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("requiresReview")
public record PaymentRequiresReview(
    UUID orderId,
    Instant processedAt,
    BigDecimal amount,
    String currency,
    String reviewReason
) implements PaymentOutcome {
    public PaymentRequiresReview {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(processedAt, "processedAt must not be null");
        amount = CommonDomainValidation.requireNonNegative(amount, "amount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
        reviewReason = CommonDomainValidation.requireNonBlank(reviewReason, "reviewReason");
    }

    @Override
    public String status() {
        return "REQUIRES_REVIEW";
    }

    @Override
    public TerminalOrderState toTerminalOrderState(Instant resolvedAt) {
        return new TerminalOrderState(
            orderId, "FAILED_COMPENSATED", resolvedAt, "manual-review", status(), null, reviewReason);
    }

    @Override
    public <R> R accept(PaymentOutcomeVisitor<R> visitor) {
        return visitor.requiresReview(this);
    }
}
