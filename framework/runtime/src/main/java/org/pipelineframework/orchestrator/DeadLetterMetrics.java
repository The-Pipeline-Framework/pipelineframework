package org.pipelineframework.orchestrator;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Dead-letter observability metrics helper.
 */
public final class DeadLetterMetrics {

    private static final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.dlq.provider");
    private static final AttributeKey<String> TRANSPORT = AttributeKey.stringKey("tpf.transport");
    private static final AttributeKey<String> PLATFORM = AttributeKey.stringKey("tpf.platform");
    private static final AttributeKey<String> TERMINAL_STATUS = AttributeKey.stringKey("tpf.execution.terminal_status");
    private static final AttributeKey<String> TERMINAL_REASON = AttributeKey.stringKey("tpf.execution.terminal_reason");
    private static final AttributeKey<String> ERROR_CODE = AttributeKey.stringKey("tpf.error.code");
    private static final AttributeKey<Boolean> RETRYABLE = AttributeKey.booleanKey("tpf.error.retryable");
    private static final AttributeKey<String> RESOURCE_TYPE = AttributeKey.stringKey("tpf.resource.type");

    private static volatile LongCounter dlqPublishCounter;

    private DeadLetterMetrics() {
    }

    /**
     * Record one dead-letter publication with standardized dimensions.
     *
     * @param provider provider name
     * @param envelope dead-letter envelope
     */
    public static void record(String provider, DeadLetterEnvelope envelope) {
        if (envelope == null) {
            return;
        }
        ensureInitialized();
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
        dlqPublishCounter.add(1, attributes);
    }

    private static void ensureInitialized() {
        if (dlqPublishCounter != null) {
            return;
        }
        synchronized (DeadLetterMetrics.class) {
            if (dlqPublishCounter != null) {
                return;
            }
            dlqPublishCounter = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                .counterBuilder("tpf.execution.dlq.publish.total")
                .setDescription("Total terminal execution failures published to dead-letter destinations")
                .setUnit("events")
                .build();
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}
