package org.pipelineframework.orchestrator;

import jakarta.inject.Singleton;

import org.pipelineframework.telemetry.TelemetryCompatibilityAccess;

import org.pipelineframework.telemetry.TelemetryRuntimes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Dead-letter observability metrics helper.
 */
@Singleton
public final class DeadLetterMetricsRecorder {

    private final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.dlq.provider");
    private final AttributeKey<String> TRANSPORT = AttributeKey.stringKey("tpf.transport");
    private final AttributeKey<String> PLATFORM = AttributeKey.stringKey("tpf.platform");
    private final AttributeKey<String> TERMINAL_STATUS = AttributeKey.stringKey("tpf.execution.terminal_status");
    private final AttributeKey<String> TERMINAL_REASON = AttributeKey.stringKey("tpf.execution.terminal_reason");
    private final AttributeKey<String> ERROR_CODE = AttributeKey.stringKey("tpf.error.code");
    private final AttributeKey<Boolean> RETRYABLE = AttributeKey.booleanKey("tpf.error.retryable");
    private final AttributeKey<String> RESOURCE_TYPE = AttributeKey.stringKey("tpf.resource.type");

    DeadLetterMetricsRecorder() {
    }

    /**
     * Record one dead-letter publication with standardized dimensions.
     *
     * @param provider provider name
     * @param envelope dead-letter envelope
     */
    public void record(String provider, DeadLetterEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        Attributes attributes = Attributes.builder()
            .put(PROVIDER, normalize(provider))
            .put(TRANSPORT, normalize(envelope.transport()))
            .put(PLATFORM, normalize(envelope.platform()))
            .put(TERMINAL_STATUS, normalize(envelope.terminalStatus()))
            .put(TERMINAL_REASON, normalize(envelope.terminalReason()))
            .put(ERROR_CODE, normalize(envelope.errorCode()))
            .put(RETRYABLE, envelope.retryable())
            .put(RESOURCE_TYPE, normalize(envelope.resourceType()))
            .build();
        counter().add(1, attributes);
    }

    private LongCounter counter() {
        return TelemetryCompatibilityAccess.metricsRuntime().meter("org.pipelineframework")
            .counterBuilder("tpf.execution.dlq.publish.total")
            .setDescription("Total terminal execution failures published to dead-letter destinations")
            .setUnit("events")
            .build();
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}
