package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TelemetryAttributesTest {
    @Test
    void metricAttributesKeepAwaitIdentityLowCardinality() {
        Map<String, String> attributes = TelemetryAttributes.metricAttributes(
            new TelemetryObservation.AwaitCompletionAdmitted("approval", "sqs", Instant.EPOCH));

        assertEquals(Map.of("tpf.await.step_id", "approval", "tpf.await.transport", "sqs"), attributes);
        assertFalse(attributes.keySet().stream().anyMatch(key -> key.matches(".*(execution|interaction|correlation|request|ordinal).*")));
    }
}
