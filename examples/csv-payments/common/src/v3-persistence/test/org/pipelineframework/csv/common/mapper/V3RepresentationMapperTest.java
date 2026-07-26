package org.pipelineframework.csv.common.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class V3RepresentationMapperTest {

    @Test
    void paymentRecordRoundTripsBetweenCanonicalAndPersistenceRepresentation() {
        var canonical = new org.pipelineframework.csv.domain.PaymentRecord(
            UUID.randomUUID(), "record-1", "Ada", new BigDecimal("12.34"),
            Currency.getInstance("EUR"), Path.of("/tmp/input.csv"));
        var mapper = new PaymentRecordRepresentationMapper();

        var representation = mapper.toExternal(canonical);

        assertEquals(canonical, mapper.fromExternal(representation));
    }

    @Test
    void paymentOutputLeavesPersistenceIdentityWithTheRepresentation() {
        var canonical = new org.pipelineframework.csv.domain.PaymentOutput(
            "payments.csv", Path.of("/tmp/input.csv"), "record-1", "Ada",
            new BigDecimal("12.34"), Currency.getInstance("EUR"), UUID.randomUUID(),
            200L, "Approved", new BigDecimal("0.99"));
        var mapper = new PaymentOutputPersistenceMapper();

        var representation = mapper.toExternal(canonical);

        assertNotNull(representation.getId());
        representation.setId(UUID.randomUUID());
        assertEquals(canonical, mapper.fromExternal(representation));
    }
}
