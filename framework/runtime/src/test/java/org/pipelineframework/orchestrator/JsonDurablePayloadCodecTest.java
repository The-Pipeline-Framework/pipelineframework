package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JsonDurablePayloadCodecTest {

    private final JsonDurablePayloadCodec codec = new JsonDurablePayloadCodec();
    private final CanonicalPayloadBinding binding = new CanonicalPayloadBinding(
        Decision.class.getSimpleName(), "expression-fingerprint", "catalog-fingerprint", Decision.class);

    @Test
    void roundTripsTheBoundCanonicalType() {
        TypedDurablePayload encoded = codec.encode(new Decision("approved"), binding);

        assertEquals(new Decision("approved"), codec.decode(encoded, binding));
    }

    @Test
    void rejectsATypeOrFingerprintMismatchBeforeDecoding() {
        TypedDurablePayload encoded = codec.encode(new Decision("approved"), binding);
        CanonicalPayloadBinding incompatible = new CanonicalPayloadBinding(
            Decision.class.getSimpleName(), "other-expression", "catalog-fingerprint", Decision.class);

        assertThrows(IllegalArgumentException.class, () -> codec.decode(encoded, incompatible));
    }

    record Decision(String result) {
    }
}
