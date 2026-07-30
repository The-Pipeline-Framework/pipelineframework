package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    @Test
    void roundTripsASealedCanonicalUnionCaseWithoutJavaClassMetadata() {
        CanonicalPayloadBinding unionBinding = new CanonicalPayloadBinding(
            DecisionStatus.class.getSimpleName(), "union-expression", "catalog-fingerprint", DecisionStatus.class);

        TypedDurablePayload encoded = codec.encode(new DecisionStatus.Approved(new Decision("approved")), unionBinding);
        Object decoded = codec.decode(encoded, unionBinding);

        assertEquals(new DecisionStatus.Approved(new Decision("approved")), decoded);
        assertFalse(new String(encoded.payload(), java.nio.charset.StandardCharsets.UTF_8)
            .contains("_tpf_java_class"));
    }

    @Test
    void protectsTheEncodedBytesFromCallerMutation() {
        byte[] bytes = new byte[] {1, 2, 3};
        TypedDurablePayload payload = new TypedDurablePayload(
            "Decision", "expression", "catalog", "application/test", 1, bytes);

        bytes[0] = 9;
        byte[] exposed = payload.payload();
        exposed[1] = 9;

        assertEquals(1, payload.payload()[0]);
        assertEquals(2, payload.payload()[1]);
    }

    @Test
    void rejectsNonIntegralOrOutOfRangeEncodingVersionsInDurableMaps() {
        for (Number invalidVersion : java.util.List.of(0, -1, 1.5d, Long.MAX_VALUE, Double.NaN)) {
            assertTrue(TypedDurablePayload.fromDurableValue(java.util.Map.of(
                "canonicalTypeId", "Decision",
                "typeExpressionFingerprint", "expression",
                "catalogFingerprint", "catalog",
                "encoding", "application/test",
                "encodingVersion", invalidVersion,
                "payload", "e30=")).isEmpty());
        }
    }

    record Decision(String result) {
    }

    sealed interface DecisionStatus permits DecisionStatus.Approved, DecisionStatus.Rejected {
        String discriminator();

        record Approved(Decision value) implements DecisionStatus {
            @Override public String discriminator() { return "approved"; }
        }

        record Rejected(Decision value) implements DecisionStatus {
            @Override public String discriminator() { return "rejected"; }
        }
    }
}
