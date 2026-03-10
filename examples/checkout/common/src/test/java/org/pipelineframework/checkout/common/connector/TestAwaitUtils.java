package org.pipelineframework.checkout.common.connector;

import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TestAwaitUtils {

    private TestAwaitUtils() {
    }

    public static void awaitUntil(BooleanSupplier condition, String message) {
        for (int i = 0; i < 50; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for condition: " + message, e);
            }
        }
        assertTrue(condition.getAsBoolean(), message);
    }
}
