package org.pipelineframework.checkout.common.connector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorUtilsTest {

    @Test
    void deterministicHandoffKeyIsStableForSameInputs() {
        String first = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "order-1", "cust-1", "2026-03-05T10:00:00Z");
        String second = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "order-1", "cust-1", "2026-03-05T10:00:00Z");

        assertEquals(first, second, "Deterministic handoff key must be stable for identical inputs.");
    }

    @Test
    void deterministicHandoffKeyDoesNotCollideOnDelimiterLikeInputs() {
        String single = ConnectorUtils.deterministicHandoffKey("handoff", "a|b");
        String split = ConnectorUtils.deterministicHandoffKey("handoff", "a", "b");

        assertNotEquals(single, split,
            "Length-prefixed seed should avoid ambiguous delimiter collisions.");
    }

    @Test
    void deterministicHandoffKeyDiffersAcrossNamespaces() {
        String create = ConnectorUtils.deterministicHandoffKey("create-to-deliver", "same", "payload");
        String deliver = ConnectorUtils.deterministicHandoffKey("deliver-to-next", "same", "payload");

        assertNotEquals(create, deliver, "Namespace must scope deterministic handoff keys.");
    }

    @Test
    void failureSignatureIsStructuredAndNormalizesBlanks() {
        String signature = ConnectorUtils.failureSignature(" ", "mapping", "missing_fields", "", "id-1");

        assertTrue(signature.contains("connector=unknown"));
        assertTrue(signature.contains("phase=mapping"));
        assertTrue(signature.contains("reason=missing_fields"));
        assertTrue(signature.contains("traceId=na"));
        assertTrue(signature.contains("itemId=id-1"));
    }
}
