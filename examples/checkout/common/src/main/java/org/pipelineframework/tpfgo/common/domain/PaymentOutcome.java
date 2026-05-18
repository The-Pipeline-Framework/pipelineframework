package org.pipelineframework.tpfgo.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PaymentCaptured.class, name = "captured"),
    @JsonSubTypes.Type(value = PaymentRejected.class, name = "rejected"),
    @JsonSubTypes.Type(value = PaymentRequiresReview.class, name = "requiresReview")
})
public sealed interface PaymentOutcome permits PaymentCaptured, PaymentRejected, PaymentRequiresReview {
    UUID orderId();

    Instant processedAt();

    BigDecimal amount();

    String currency();

    String status();

    TerminalOrderState toTerminalOrderState(Instant resolvedAt);

    <R> R accept(PaymentOutcomeVisitor<R> visitor);
}
