package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.pipelineframework.objectingest.ObjectIngestTelemetry;

/**
 * Bridges Object Ingest lifecycle hooks into replay telemetry.
 */
@ApplicationScoped
public class ObjectIngestReplayTelemetry implements ObjectIngestTelemetry {

    private static final String STEP = "ObjectIngest";
    private static final String SERVICE = "ObjectIngestConnector";
    private static final AttributeKey<String> SOURCE = AttributeKey.stringKey("tpf.object_ingest.source");
    private static final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.object_ingest.provider");

    private final LongCounter listedCounter;
    private final LongCounter listedObjectCounter;
    private final LongCounter submittedCounter;
    private final LongCounter duplicateCounter;
    private final LongCounter failedCounter;

    @Inject
    PipelineTelemetry telemetry;

    public ObjectIngestReplayTelemetry() {
        var meter = GlobalOpenTelemetry.getMeter("org.pipelineframework");
        listedCounter = meter.counterBuilder("tpf.object_ingest.list.total")
            .setDescription("Total Object Ingest listing attempts")
            .setUnit("events")
            .build();
        listedObjectCounter = meter.counterBuilder("tpf.object_ingest.listed.objects.total")
            .setDescription("Total objects returned by Object Ingest listing")
            .setUnit("objects")
            .build();
        submittedCounter = meter.counterBuilder("tpf.object_ingest.submitted.total")
            .setDescription("Total object inputs submitted as queue-async executions")
            .setUnit("objects")
            .build();
        duplicateCounter = meter.counterBuilder("tpf.object_ingest.duplicate.total")
            .setDescription("Total duplicate object admissions detected")
            .setUnit("objects")
            .build();
        failedCounter = meter.counterBuilder("tpf.object_ingest.failed.total")
            .setDescription("Total Object Ingest failures")
            .setUnit("objects")
            .build();
    }

    @Override
    public void listed(String sourceName, String provider, int count) {
        Attributes attributes = metricAttributes(sourceName, provider);
        listedCounter.add(1, attributes);
        listedObjectCounter.add(Math.max(0, count), attributes);
        emit("object_ingest_listed", sourceName, provider, null, Map.of("count", Integer.toString(count)));
    }

    @Override
    public void submitted(String sourceName, String provider, String key) {
        submittedCounter.add(1, metricAttributes(sourceName, provider));
        emit("object_ingest_submitted", sourceName, provider, key, Map.of());
    }

    @Override
    public void duplicate(String sourceName, String provider, String key) {
        duplicateCounter.add(1, metricAttributes(sourceName, provider));
        emit("object_ingest_duplicate", sourceName, provider, key, Map.of());
    }

    @Override
    public void failed(String sourceName, String provider, String key, Throwable failure) {
        failedCounter.add(1, metricAttributes(sourceName, provider));
        Map<String, String> attributes = new LinkedHashMap<>();
        if (failure != null) {
            attributes.put("errorType", failure.getClass().getName());
            if (failure.getMessage() != null) {
                attributes.put("errorMessage", failure.getMessage());
            }
        }
        emit("object_ingest_failed", sourceName, provider, key, attributes);
    }

    private void emit(
        String event,
        String sourceName,
        String provider,
        String key,
        Map<String, String> extraAttributes
    ) {
        Map<String, String> attributes = new LinkedHashMap<>();
        put(attributes, "connector", "object-ingest");
        put(attributes, "source", sourceName);
        put(attributes, "provider", provider);
        put(attributes, "key", key);
        if (extraAttributes != null) {
            attributes.putAll(extraAttributes);
        }
        if (telemetry != null) {
            telemetry.recordConnectorReplayEvent(STEP, SERVICE, event, STEP, null, attributes);
        }
    }

    private static Attributes metricAttributes(String sourceName, String provider) {
        return Attributes.builder()
            .put(SOURCE, normalize(sourceName))
            .put(PROVIDER, normalize(provider))
            .build();
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
