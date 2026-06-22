package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.objectingest.ObjectIngestTelemetry;

/**
 * Bridges Object Ingest lifecycle hooks into replay telemetry.
 */
@ApplicationScoped
public class ObjectIngestReplayTelemetry implements ObjectIngestTelemetry {

    private static final String STEP = "ObjectIngest";
    private static final String SERVICE = "ObjectIngestConnector";

    @Inject
    PipelineTelemetry telemetry;

    @Override
    public void listed(String sourceName, String provider, int count) {
        emit("object_ingest_listed", sourceName, provider, null, Map.of("count", Integer.toString(count)));
    }

    @Override
    public void submitted(String sourceName, String provider, String key) {
        emit("object_ingest_submitted", sourceName, provider, key, Map.of());
    }

    @Override
    public void duplicate(String sourceName, String provider, String key) {
        emit("object_ingest_duplicate", sourceName, provider, key, Map.of());
    }

    @Override
    public void failed(String sourceName, String provider, String key, Throwable failure) {
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
        telemetry.recordConnectorReplayEvent(STEP, SERVICE, event, STEP, null, attributes);
    }

    private static void put(Map<String, String> attributes, String key, String value) {
        if (value != null && !value.isBlank()) {
            attributes.put(key, value);
        }
    }
}
