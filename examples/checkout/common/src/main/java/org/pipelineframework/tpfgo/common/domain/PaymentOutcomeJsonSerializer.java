package org.pipelineframework.tpfgo.common.domain;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public final class PaymentOutcomeJsonSerializer extends JsonSerializer<PaymentOutcome> {

    @Override
    public void serialize(PaymentOutcome value, JsonGenerator generator, SerializerProvider serializers) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", value.type());
        generator.writeFieldName(value.payloadFieldName());
        generator.writeStartObject();
        value.writePayload(generator);
        generator.writeEndObject();
        generator.writeEndObject();
    }
}
