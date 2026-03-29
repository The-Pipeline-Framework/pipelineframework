package org.pipelineframework.tpfgo.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class GrpcMappingSupportTest {

    // --- uuid ---

    @Test
    void uuidReturnsNullForNullInput() {
        assertNull(GrpcMappingSupport.uuid(null, "field"));
    }

    @Test
    void uuidReturnsNullForBlankInput() {
        assertNull(GrpcMappingSupport.uuid("   ", "field"));
        assertNull(GrpcMappingSupport.uuid("", "field"));
    }

    @Test
    void uuidParsesValidString() {
        String value = "550e8400-e29b-41d4-a716-446655440000";
        UUID result = GrpcMappingSupport.uuid(value, "id");
        assertEquals(UUID.fromString(value), result);
    }

    @Test
    void uuidThrowsOnInvalidString() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> GrpcMappingSupport.uuid("not-a-uuid", "myField"));
        assertTrue(ex.getMessage().contains("myField"));
        assertTrue(ex.getMessage().contains("not-a-uuid"));
    }

    // --- str(UUID) ---

    @Test
    void strUuidReturnsEmptyStringForNull() {
        assertEquals("", GrpcMappingSupport.str((UUID) null));
    }

    @Test
    void strUuidReturnsStringRepresentation() {
        UUID id = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        assertEquals("12345678-1234-1234-1234-123456789abc", GrpcMappingSupport.str(id));
    }

    @Test
    void strUuidRoundTrips() {
        UUID original = UUID.randomUUID();
        assertEquals(original, UUID.fromString(GrpcMappingSupport.str(original)));
    }

    // --- instant ---

    @Test
    void instantReturnsNullForNullInput() {
        assertNull(GrpcMappingSupport.instant(null, "field"));
    }

    @Test
    void instantReturnsNullForBlankInput() {
        assertNull(GrpcMappingSupport.instant("", "ts"));
        assertNull(GrpcMappingSupport.instant("   ", "ts"));
    }

    @Test
    void instantParsesIso8601String() {
        String value = "2026-03-15T09:30:00Z";
        Instant result = GrpcMappingSupport.instant(value, "createdAt");
        assertEquals(Instant.parse(value), result);
    }

    @Test
    void instantThrowsOnInvalidString() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> GrpcMappingSupport.instant("not-a-date", "myTs"));
        assertTrue(ex.getMessage().contains("myTs"));
        assertTrue(ex.getMessage().contains("not-a-date"));
    }

    // --- str(Instant) ---

    @Test
    void strInstantReturnsEmptyStringForNull() {
        assertEquals("", GrpcMappingSupport.str((Instant) null));
    }

    @Test
    void strInstantReturnsIso8601String() {
        Instant instant = Instant.parse("2026-01-01T00:00:00Z");
        assertEquals("2026-01-01T00:00:00Z", GrpcMappingSupport.str(instant));
    }

    @Test
    void strInstantRoundTrips() {
        Instant original = Instant.parse("2026-06-15T12:34:56.789Z");
        assertEquals(original, Instant.parse(GrpcMappingSupport.str(original)));
    }

    // --- decimal ---

    @Test
    void decimalReturnsNullForNullInput() {
        assertNull(GrpcMappingSupport.decimal(null, "field"));
    }

    @Test
    void decimalReturnsNullForBlankInput() {
        assertNull(GrpcMappingSupport.decimal("", "amount"));
        assertNull(GrpcMappingSupport.decimal("   ", "amount"));
    }

    @Test
    void decimalParsesValidDecimalString() {
        BigDecimal result = GrpcMappingSupport.decimal("42.50", "totalAmount");
        assertEquals(new BigDecimal("42.50"), result);
    }

    @Test
    void decimalParsesZero() {
        BigDecimal result = GrpcMappingSupport.decimal("0", "totalAmount");
        assertEquals(BigDecimal.ZERO, result.stripTrailingZeros());
    }

    @Test
    void decimalParsesNegativeValue() {
        BigDecimal result = GrpcMappingSupport.decimal("-10.99", "amount");
        assertEquals(new BigDecimal("-10.99"), result);
    }

    @Test
    void decimalThrowsOnInvalidString() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> GrpcMappingSupport.decimal("not-a-number", "price"));
        assertTrue(ex.getMessage().contains("price"));
        assertTrue(ex.getMessage().contains("not-a-number"));
    }

    // --- str(BigDecimal) ---

    @Test
    void strDecimalReturnsEmptyStringForNull() {
        assertEquals("", GrpcMappingSupport.str((BigDecimal) null));
    }

    @Test
    void strDecimalReturnsPlainString() {
        BigDecimal value = new BigDecimal("12345.678");
        assertEquals("12345.678", GrpcMappingSupport.str(value));
    }

    @Test
    void strDecimalDoesNotUseScientificNotation() {
        BigDecimal value = new BigDecimal("0.000001");
        String result = GrpcMappingSupport.str(value);
        assertEquals("0.000001", result);
    }

    @Test
    void strDecimalRoundTrips() {
        BigDecimal original = new BigDecimal("99.95");
        assertEquals(original, new BigDecimal(GrpcMappingSupport.str(original)));
    }

    // Regression: very large decimal round-trip
    @Test
    void strDecimalRoundTripsLargeValue() {
        BigDecimal original = new BigDecimal("9999999999999.9999");
        assertEquals(original, new BigDecimal(GrpcMappingSupport.str(original)));
    }

}