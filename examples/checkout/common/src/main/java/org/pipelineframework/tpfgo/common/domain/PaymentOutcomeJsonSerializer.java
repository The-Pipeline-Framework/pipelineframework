package org.pipelineframework.tpfgo.common.domain;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public final class PaymentOutcomeJsonSerializer extends JsonSerializer<PaymentOutcome> {

    @Override
    public void serialize(PaymentOutcome value, JsonGenerator generator, SerializerProvider serializers) throws IOException {
        generator.writeStartObject();
        if (value instanceof PaymentCaptured captured) {
            generator.writeStringField("type", "captured");
            generator.writeStringField("orderId", captured.orderId().toString());
            generator.writeStringField("paymentId", captured.paymentId().toString());
            generator.writeStringField("processedAt", captured.processedAt().toString());
            generator.writeStringField("amount", captured.amount().toPlainString());
            generator.writeStringField("currency", captured.currency());
        } else if (value instanceof PaymentRejected rejected) {
            generator.writeStringField("type", "rejected");
            generator.writeStringField("orderId", rejected.orderId().toString());
            generator.writeStringField("processedAt", rejected.processedAt().toString());
            generator.writeStringField("amount", rejected.amount().toPlainString());
            generator.writeStringField("currency", rejected.currency());
            generator.writeStringField("failureCode", rejected.failureCode());
            generator.writeStringField("failureReason", rejected.failureReason());
        } else if (value instanceof PaymentRequiresReview review) {
            generator.writeStringField("type", "requiresReview");
            generator.writeStringField("orderId", review.orderId().toString());
            generator.writeStringField("processedAt", review.processedAt().toString());
            generator.writeStringField("amount", review.amount().toPlainString());
            generator.writeStringField("currency", review.currency());
            generator.writeStringField("reviewReason", review.reviewReason());
        } else {
            throw new IOException("Unsupported PaymentOutcome type: " + value.getClass().getName());
        }
        generator.writeEndObject();
    }
}
