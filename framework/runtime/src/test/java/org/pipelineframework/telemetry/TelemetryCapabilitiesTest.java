package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class TelemetryCapabilitiesTest {
    @Test
    void policyCannotEnableAAbsentBinaryCapability() {
        TelemetryPolicy policy = new TelemetryPolicy(true, true, true, false, false, false,
            Duration.ofSeconds(30), 10d, 3, RetryAmplificationGuardMode.FAIL_FAST);
        TelemetryCapabilities capabilities = new TelemetryCapabilities(true, false, true, false, true);

        assertFalse(capabilities.effectiveTracing(policy));
        assertTrue(capabilities.effectiveMetrics(policy));
    }
}
