package org.pipelineframework.awaitable;

import java.util.Arrays;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Normalizes await payloads into JSON-safe snapshots for persistence and transport envelopes.
 */
public final class AwaitPayloadSupport {

    private static final JsonFormat.Printer PROTO_JSON = JsonFormat.printer().omittingInsignificantWhitespace();

    private AwaitPayloadSupport() {
    }

    public static Class<?> resolvePayloadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        if (className == null || className.isBlank()) {
            throw new ClassNotFoundException("Await payload class name must not be blank.");
        }
        ClassLoader effectiveLoader = classLoader == null
            ? Thread.currentThread().getContextClassLoader()
            : classLoader;
        try {
            return effectiveLoader.loadClass(className);
        } catch (ClassNotFoundException ignored) {
            String[] segments = className.split("\\.");
            for (int boundary = segments.length - 1; boundary > 0; boundary--) {
                String packageName = String.join(".", Arrays.copyOfRange(segments, 0, boundary));
                String nestedTypeName = String.join("$", Arrays.copyOfRange(segments, boundary, segments.length));
                String candidate = packageName + "." + nestedTypeName;
                try {
                    return effectiveLoader.loadClass(candidate);
                } catch (ClassNotFoundException nestedIgnored) {
                    // Keep trying shorter package prefixes so canonical nested-class names can be resolved.
                }
            }
            throw ignored;
        }
    }

    public static Object coercePayload(Object payload, Class<?> outputType) {
        if (payload == null || outputType == null) {
            return payload;
        }
        if (outputType.isInstance(payload)) {
            return payload;
        }
        try {
            if (Message.class.isAssignableFrom(outputType)) {
                String json = payload instanceof String text
                    ? text
                    : PipelineJson.mapper().writeValueAsString(payload);
                Message.Builder builder = (Message.Builder) outputType.getMethod("newBuilder").invoke(null);
                JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
                return builder.build();
            }
            if (payload instanceof String text) {
                String trimmed = text.trim();
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    return PipelineJson.mapper().readValue(trimmed, outputType);
                }
            }
            return PipelineJson.mapper().convertValue(payload, outputType);
        } catch (Exception e) {
            throw new IllegalStateException("Failed converting await payload to " + outputType.getName(), e);
        }
    }

    public static Object normalize(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof String || payload instanceof Number || payload instanceof Boolean) {
            return payload;
        }
        try {
            var json = PipelineJson.mapper();
            var tree = payload instanceof MessageOrBuilder protobuf
                ? json.readTree(PROTO_JSON.print(protobuf))
                : json.valueToTree(payload);
            return json.treeToValue(tree, Object.class);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to normalize await payload from " + payload.getClass().getName(),
                e);
        }
    }
}
