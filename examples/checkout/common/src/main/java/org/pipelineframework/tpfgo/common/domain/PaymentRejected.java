package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("rejected")
public record PaymentRejected(
    UUID orderId,
    Instant processedAt,
    BigDecimal amount,
    String currency,
    String failureCode,
    String failureReason
) implements PaymentOutcome {
    public PaymentRejected {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(processedAt, "processedAt must not be null");
        amount = CommonDomainValidation.requireNonNegative(amount, "amount");
        currency = CommonDomainValidation.requireCurrencyCode(currency, "currency");
        failureCode = CommonDomainValidation.requireNonBlank(failureCode, "failureCode");
        failureReason = CommonDomainValidation.requireNonBlank(failureReason, "failureReason");
    }

    @Override
    public String status() {
        return "FAILED";
    }

    @Override
    public TerminalOrderState toTerminalOrderState(Instant resolvedAt) {
        return new TerminalOrderState(orderId, "FAILED_COMPENSATED", resolvedAt, "manual-review", status(), null, failureCode);
    }

    @Override
    public <R> R accept(PaymentOutcomeVisitor<R> visitor) {
        return visitor.rejected(this);
    }
}
