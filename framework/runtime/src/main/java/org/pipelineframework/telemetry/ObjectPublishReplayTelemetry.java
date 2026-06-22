package org.pipelineframework.telemetry;

import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.objectpublish.ObjectPublishTelemetry;

/**
 * Bridges Object Publish lifecycle hooks into replay telemetry.
 */
@ApplicationScoped
public class ObjectPublishReplayTelemetry implements ObjectPublishTelemetry {

    private static final String STEP = "ObjectPublish";
    private static final String SERVICE = "ObjectPublishConnector";

    @Inject
    PipelineTelemetry telemetry;

    @Override
    public void grouped(String targetName, int itemCount, int groupCount) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("itemCount", Integer.toString(itemCount));
        attributes.put("groupCount", Integer.toString(groupCount));
        emit("object_publish_grouped", targetName, null, null, null, attributes);
    }

    @Override
    public void published(String targetName, String provider, String objectKey, long bytes) {
        emit("object_publish_published", targetName, provider, objectKey, Long.toString(bytes), Map.of());
    }

    @Override
    public void skipped(String targetName) {
        emit("object_publish_skipped", targetName, null, null, null, Map.of());
    }

    @Override
    public void failed(String targetName, String provider, String objectKey, Throwable failure) {
        Map<String, String> attributes = new LinkedHashMap<>();
        if (failure != null) {
            attributes.put("errorType", failure.getClass().getName());
            if (failure.getMessage() != null) {
                attributes.put("errorMessage", failure.getMessage());
            }
        }
        emit("object_publish_failed", targetName, provider, objectKey, null, attributes);
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
        telemetry.recordConnectorReplayEvent(STEP, SERVICE, event, null, STEP, attributes);
    }

    private static void put(Map<String, String> attributes, String key, String value) {
        if (value != null && !value.isBlank()) {
            attributes.put(key, value);
        }
    }
}
