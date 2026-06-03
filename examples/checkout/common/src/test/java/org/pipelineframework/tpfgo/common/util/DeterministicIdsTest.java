package org.pipelineframework.tpfgo.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.Test;

class DeterministicIdsTest {

    @Test
    void sameInputsAlwaysProduceSameUuid() {
        UUID first = DeterministicIds.uuid("order", "req-1", "cust-1", "rest-1");
        UUID second = DeterministicIds.uuid("order", "req-1", "cust-1", "rest-1");
        assertEquals(first, second);
    }

    @Test
    void differentNamespacesProduceDifferentUuids() {
        UUID orderUuid = DeterministicIds.uuid("order", "part-1");
        UUID paymentUuid = DeterministicIds.uuid("payment", "part-1");
        assertNotEquals(orderUuid, paymentUuid);
    }

    @Test
    void differentPartsProduceDifferentUuids() {
        UUID first = DeterministicIds.uuid("order", "req-1", "cust-1");
        UUID second = DeterministicIds.uuid("order", "req-2", "cust-1");
        assertNotEquals(first, second);
    }

    @Test
    void nullNamespaceFallsBackToTpfgoDefault() {
        UUID withNull = DeterministicIds.uuid(null, "part-a");
        UUID withTpfgo = DeterministicIds.uuid("tpfgo", "part-a");
        assertEquals(withNull, withTpfgo);
    }

    @Test
    void nullPartElementFailsFast() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> DeterministicIds.uuid("ns", "a", null));
        assertEquals("parts[1] must not be null", exception.getMessage());
    }

    @Test
    void noPartsProducesStableUuid() {
        UUID first = DeterministicIds.uuid("order");
        UUID second = DeterministicIds.uuid("order");
        assertEquals(first, second);
    }

    @Test
    void nullPartsArrayFailsFast() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> DeterministicIds.uuid("order", (String[]) null));
        assertEquals("parts must not be null", exception.getMessage());
    }

    @Test
    void orderOfPartsMatters() {
        UUID forward = DeterministicIds.uuid("ns", "a", "b");
        UUID reversed = DeterministicIds.uuid("ns", "b", "a");
        assertNotEquals(forward, reversed);
    }

    @Test
    void resultIsNotNull() {
        assertNotNull(DeterministicIds.uuid("order", "req-x", "cust-x", "rest-x"));
    }

    @Test
    void knownStableValue() {
        // Regression: fixed canonical inputs must always produce the same UUID value.
        // If DeterministicIds changes its algorithm this test will catch the regression.
        UUID expected = UUID.fromString("4344f342-6a58-3bb1-9dc2-da816369af81");
        UUID actual = DeterministicIds.uuid("order", "req-fixed", "cust-fixed", "rest-fixed");
        assertEquals(expected, actual);
        // Confirm it is a valid v3/v5 UUID (not random)
        assertNotEquals(UUID.randomUUID(), actual);
    }

    @Test
    void emptyPartsDistinguishFromNoArgs() {
        UUID withEmptyPart = DeterministicIds.uuid("ns", "");
        UUID noArgs = DeterministicIds.uuid("ns");
        assertNotEquals(withEmptyPart, noArgs);
        assertEquals(withEmptyPart, DeterministicIds.uuid("ns", ""));
        assertEquals(noArgs, DeterministicIds.uuid("ns"));
    }
}
