package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import org.pipelineframework.objectpublish.ObjectPublishTelemetry;

/**
 * Bridges Object Publish lifecycle hooks into replay telemetry.
 */
@ApplicationScoped
public class ObjectPublishReplayTelemetry implements ObjectPublishTelemetry {

    private static final String STEP = "ObjectPublish";
    private static final String SERVICE = "ObjectPublishConnector";
    private static final AttributeKey<String> TARGET = AttributeKey.stringKey("tpf.object_publish.target");
    private static final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.object_publish.provider");

    private final LongCounter groupedCounter;
    private final LongCounter groupedItemsCounter;
    private final LongCounter groupedGroupsCounter;
    private final LongCounter publishedCounter;
    private final LongCounter publishedBytesCounter;
    private final LongCounter skippedCounter;
    private final LongCounter failedCounter;
    private final DoubleHistogram writeDurationHistogram;

    private final PipelineReplayTelemetry replayTelemetry;
    private final TelemetryPolicy policy;
    private final TelemetryRuntime runtime;

    @Inject
    public ObjectPublishReplayTelemetry(
        TelemetryRuntime runtime, TelemetryPolicySource policySource, PipelineReplayTelemetry replayTelemetry) {
        this(runtime, policySource.telemetryPolicy(), replayTelemetry);
    }

    public ObjectPublishReplayTelemetry(TelemetryRuntime runtime) {
        this(runtime, new TelemetryPolicy(true, true, true, false, false, false,
            Duration.ofSeconds(30), 10d, 3, RetryAmplificationGuardMode.FAIL_FAST), null);
    }

    private ObjectPublishReplayTelemetry(
        TelemetryRuntime runtime, TelemetryPolicy policy, PipelineReplayTelemetry replayTelemetry) {
        this.runtime = runtime;
        this.policy = policy;
        this.replayTelemetry = replayTelemetry;
        var meter = runtime.meter("org.pipelineframework");
        groupedCounter = meter.counterBuilder("tpf.object_publish.grouped.total")
            .setDescription("Total Object Publish grouping operations")
            .setUnit("events")
            .build();
        groupedItemsCounter = meter.counterBuilder("tpf.object_publish.grouped.items.total")
            .setDescription("Total terminal items grouped for Object Publish")
            .setUnit("items")
            .build();
        groupedGroupsCounter = meter.counterBuilder("tpf.object_publish.grouped.groups.total")
            .setDescription("Total object groups produced for Object Publish")
            .setUnit("groups")
            .build();
        publishedCounter = meter.counterBuilder("tpf.object_publish.published.total")
            .setDescription("Total objects successfully published")
            .setUnit("objects")
            .build();
        publishedBytesCounter = meter.counterBuilder("tpf.object_publish.published.bytes.total")
            .setDescription("Total bytes successfully published")
            .setUnit("bytes")
            .build();
        skippedCounter = meter.counterBuilder("tpf.object_publish.skipped.total")
            .setDescription("Total empty Object Publish outputs skipped")
            .setUnit("events")
            .build();
        failedCounter = meter.counterBuilder("tpf.object_publish.failed.total")
            .setDescription("Total Object Publish failures")
            .setUnit("objects")
            .build();
        writeDurationHistogram = meter.histogramBuilder("tpf.object_publish.write.duration")
            .setDescription("Object Publish provider write duration")
            .setUnit("ms")
            .build();
    }

    @Override
    public void grouped(String targetName, int itemCount, int groupCount) {
        Attributes metricAttributes = metricAttributes(targetName, null);
        if (policy.metricsEnabled()) {
            groupedCounter.add(1, metricAttributes);
            groupedItemsCounter.add(Math.max(0, itemCount), metricAttributes);
            groupedGroupsCounter.add(Math.max(0, groupCount), metricAttributes);
        }
        Map<String, String> replayAttributes = new LinkedHashMap<>();
        replayAttributes.put("itemCount", Integer.toString(itemCount));
        replayAttributes.put("groupCount", Integer.toString(groupCount));
        emit("object_publish_grouped", targetName, null, null, null, replayAttributes);
    }

    @Override
    public void published(String targetName, String provider, String objectKey, long bytes) {
        Attributes attributes = metricAttributes(targetName, provider);
        if (policy.metricsEnabled()) {
            publishedCounter.add(1, attributes);
            publishedBytesCounter.add(Math.max(0L, bytes), attributes);
        }
        emit("object_publish_published", targetName, provider, objectKey, Long.toString(bytes), Map.of());
        if (!policy.tracingEnabled()) return;
        Span span = runtime.tracer("org.pipelineframework")
            .spanBuilder("tpf.terminal.publication.completed")
            .startSpan();
        try {
            span.setAttribute("tpf.object_publish.target", normalize(targetName));
            span.setAttribute("tpf.object_publish.provider", normalize(provider));
        } finally {
            span.end();
        }
    }

    @Override
    public void skipped(String targetName) {
        if (policy.metricsEnabled()) skippedCounter.add(1, metricAttributes(targetName, null));
        emit("object_publish_skipped", targetName, null, null, null, Map.of());
    }

    @Override
    public void failed(String targetName, String provider, String objectKey, Throwable failure) {
        if (policy.metricsEnabled()) failedCounter.add(1, metricAttributes(targetName, provider));
        Map<String, String> attributes = new LinkedHashMap<>();
        if (failure != null) {
            attributes.put("errorType", failure.getClass().getName());
            if (failure.getMessage() != null) {
                attributes.put("errorMessage", failure.getMessage());
            }
        }
        emit("object_publish_failed", targetName, provider, objectKey, null, attributes);
        if (!policy.tracingEnabled()) return;
        Span span = runtime.tracer("org.pipelineframework")
            .spanBuilder("tpf.terminal.publication.failed")
            .startSpan();
        try {
            span.setAttribute("tpf.object_publish.target", normalize(targetName));
            span.setAttribute("tpf.object_publish.provider", normalize(provider));
            span.setStatus(StatusCode.ERROR);
            if (failure != null) {
                span.recordException(failure);
            }
        } finally {
            span.end();
        }
    }

    @Override
    public void writeDuration(String targetName, String provider, Duration duration) {
        if (duration == null) {
            return;
        }
        if (policy.metricsEnabled()) {
            writeDurationHistogram.record(
                Math.max(0.0d, duration.toNanos() / 1_000_000.0d),
                metricAttributes(targetName, provider));
        }
    }

    private void emit(
        String event,
        String targetName,
        String provider,
        String objectKey,
        String bytes,
        Map<String, String> extraAttributes
    ) {
        Map<String, String> attributes = new LinkedHashMap<>();
        put(attributes, "connector", "object-publish");
        put(attributes, "target", targetName);
        put(attributes, "provider", provider);
        put(attributes, "key", objectKey);
        put(attributes, "bytes", bytes);
        if (extraAttributes != null) {
            attributes.putAll(extraAttributes);
        }
        if (policy.replayEnabled() && replayTelemetry != null) {
            replayTelemetry.recordConnectorReplayEvent(STEP, SERVICE, event, null, STEP, attributes);
        }
    }

    private static Attributes metricAttributes(String targetName, String provider) {
        AttributesBuilder builder = Attributes.builder()
            .put(TARGET, normalize(targetName));
        if (provider != null && !provider.isBlank()) {
            builder.put(PROVIDER, provider.trim());
        }
        return builder.build();
    }

    private static void put(Map<String, String> attributes, String key, String value) {
        if (value != null && !value.isBlank()) {
            attributes.put(key, value);
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value.trim();
    }
}
