package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineStepConfig;

class TelemetryPolicyTest {
    @Test
    void resolvesSignalsOnceAndKeepsReplayDependentOnTracingAndTopology() {
        PipelineStepConfig config = mock(PipelineStepConfig.class);
        PipelineStepConfig.TelemetryConfig telemetry = mock(PipelineStepConfig.TelemetryConfig.class);
        PipelineStepConfig.TracingConfig tracing = mock(PipelineStepConfig.TracingConfig.class);
        PipelineStepConfig.MetricsConfig metrics = mock(PipelineStepConfig.MetricsConfig.class);
        PipelineStepConfig.ReplayConfig replay = mock(PipelineStepConfig.ReplayConfig.class);
        when(config.telemetry()).thenReturn(telemetry);
        when(config.killSwitch()).thenReturn(null);
        when(telemetry.enabled()).thenReturn(true);
        when(telemetry.tracing()).thenReturn(tracing);
        when(telemetry.metrics()).thenReturn(metrics);
        when(telemetry.replay()).thenReturn(replay);
        when(tracing.enabled()).thenReturn(true);
        when(tracing.perItem()).thenReturn(true);
        when(metrics.enabled()).thenReturn(true);
        when(replay.enabled()).thenReturn(true);
        when(replay.exporter()).thenReturn("file");
        when(replay.filePath()).thenReturn(java.util.Optional.of("/tmp/replay.json"));

        TelemetryPolicy policy = TelemetryPolicy.from(config, true);

        assertTrue(policy.metricsEnabled());
        assertTrue(policy.tracingEnabled());
        assertTrue(policy.perItemSpansEnabled());
        assertTrue(policy.replayEnabled());
        assertFalse(TelemetryPolicy.from(config, false).replayEnabled());
    }
}
