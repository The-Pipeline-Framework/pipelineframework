package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = PaymentOutcomeJsonSerializer.class)
@JsonDeserialize(using = PaymentOutcomeJsonDeserializer.class)
public sealed interface PaymentOutcome permits PaymentCaptured, PaymentRejected, PaymentRequiresReview {
    UUID orderId();

    Instant processedAt();

    BigDecimal amount();

    String currency();

    String status();

    TerminalOrderState toTerminalOrderState(Instant resolvedAt);

    <R> R accept(PaymentOutcomeVisitor<R> visitor);
}
