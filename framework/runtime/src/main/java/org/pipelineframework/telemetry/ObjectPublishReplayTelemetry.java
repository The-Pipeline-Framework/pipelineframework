package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
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

    @Inject
    PipelineTelemetry telemetry;

    public ObjectPublishReplayTelemetry() {
        var meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
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
        groupedCounter.add(1, metricAttributes);
        groupedItemsCounter.add(Math.max(0, itemCount), metricAttributes);
        groupedGroupsCounter.add(Math.max(0, groupCount), metricAttributes);
        Map<String, String> replayAttributes = new LinkedHashMap<>();
        replayAttributes.put("itemCount", Integer.toString(itemCount));
        replayAttributes.put("groupCount", Integer.toString(groupCount));
        emit("object_publish_grouped", targetName, null, null, null, replayAttributes);
    }

    @Override
    public void published(String targetName, String provider, String objectKey, long bytes) {
        Attributes attributes = metricAttributes(targetName, provider);
        publishedCounter.add(1, attributes);
        publishedBytesCounter.add(Math.max(0L, bytes), attributes);
        emit("object_publish_published", targetName, provider, objectKey, Long.toString(bytes), Map.of());
    }

    @Override
    public void skipped(String targetName) {
        skippedCounter.add(1, metricAttributes(targetName, null));
        emit("object_publish_skipped", targetName, null, null, null, Map.of());
    }

    @Override
    public void failed(String targetName, String provider, String objectKey, Throwable failure) {
        failedCounter.add(1, metricAttributes(targetName, provider));
        Map<String, String> attributes = new LinkedHashMap<>();
        if (failure != null) {
            attributes.put("errorType", failure.getClass().getName());
            if (failure.getMessage() != null) {
                attributes.put("errorMessage", failure.getMessage());
            }
        }
        emit("object_publish_failed", targetName, provider, objectKey, null, attributes);
    }

    @Override
    public void writeDuration(String targetName, String provider, Duration duration) {
        if (duration == null) {
            return;
        }
        writeDurationHistogram.record(
            Math.max(0.0d, duration.toNanos() / 1_000_000.0d),
            metricAttributes(targetName, provider));
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
        if (telemetry != null) {
            telemetry.recordConnectorReplayEvent(STEP, SERVICE, event, null, STEP, attributes);
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
