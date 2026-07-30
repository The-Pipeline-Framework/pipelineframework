package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
