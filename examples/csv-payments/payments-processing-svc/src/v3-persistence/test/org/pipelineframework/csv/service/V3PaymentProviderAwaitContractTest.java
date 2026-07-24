package org.pipelineframework.csv.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.domain.PaymentRecord;
import org.pipelineframework.csv.domain.PaymentStatus;
import org.pipelineframework.csv.domain.PipelineDomainProtoAdapters;

class V3PaymentProviderAwaitContractTest {

    @Test
    void generatedAwaitPayloadRoundTripsThroughTheProviderBoundary() {
        PaymentRecord record = new PaymentRecord(
            UUID.randomUUID(), "csv-1", "Ada", new BigDecimal("12.34"),
            Currency.getInstance("EUR"), Path.of("/tmp/payments.csv"));
        var provider = new PaymentProviderServiceMock(new PaymentProviderServiceMockTest.FakePaymentProviderConfig());

        PaymentStatus status = provider.processPayment(
            PipelineDomainProtoAdapters.fromProto(PipelineDomainProtoAdapters.toProto(record)));

        PaymentStatus rebuilt = PipelineDomainProtoAdapters.fromProto(PipelineDomainProtoAdapters.toProto(status));
        var approved = assertInstanceOf(PaymentStatus.Approved.class, rebuilt);
        assertEquals(record.id(), approved.value().paymentRecordId());
        assertEquals(record, approved.value().paymentRecord());
    }
}
