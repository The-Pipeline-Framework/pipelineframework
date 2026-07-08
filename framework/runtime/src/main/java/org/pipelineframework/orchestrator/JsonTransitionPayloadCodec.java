package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.nio.file.Path;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Internal JSON codec for local transition envelopes.
 */
@ApplicationScoped
public class JsonTransitionPayloadCodec implements TransitionPayloadCodec {

    public static final String ENCODING = "application/tpf-transition+json";
    private static final String NULL_TYPE_ID = "null";
    private static final String LIST_TYPE_ID = "java.util.List";
    private static final String MAP_TYPE_ID = "java.util.Map";
    private static final String SET_TYPE_ID = "java.util.Set";
    private static final List<String> DEFAULT_APPLICATION_PAYLOAD_PREFIXES = List.of("org.pipelineframework.");
    private static final Set<String> SAFE_JDK_PAYLOAD_TYPES = Set.of(
        Boolean.class.getName(),
        Byte.class.getName(),
        Character.class.getName(),
        Double.class.getName(),
        Float.class.getName(),
        Integer.class.getName(),
        Long.class.getName(),
        Short.class.getName(),
        String.class.getName(),
        java.math.BigDecimal.class.getName(),
        java.math.BigInteger.class.getName(),
        java.time.Instant.class.getName(),
        java.time.LocalDate.class.getName(),
        java.time.LocalDateTime.class.getName(),
        java.time.OffsetDateTime.class.getName(),
        java.time.ZonedDateTime.class.getName(),
        java.nio.file.Path.class.getName(),
        java.util.UUID.class.getName());

    private final ObjectMapper mapper = PipelineJson.mapper();

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Override
    public String encoding() {
        return ENCODING;
    }

    @Override
    public SerializedTransitionPayload encode(Object payload) {
        if (payload instanceof ExecutionInputSnapshot snapshot) {
            return encodeExecutionInputSnapshot(snapshot);
        }
        if (payload instanceof List<?> list) {
            return encodeList(list);
        }
        if (payload instanceof Set<?> set) {
            return encodeSet(set);
        }
        if (payload instanceof Map<?, ?> map) {
            return encodeMap(map);
        }
        if (payload != null && payload.getClass().isArray()) {
            return encodeArray(payload);
        }
        Class<?> payloadClass = payload instanceof Path ? Path.class : payload == null ? null : payload.getClass();
        String payloadTypeId = payloadClass == null ? NULL_TYPE_ID : payloadClass.getName();
        if (payload != null && !isAllowedPayloadType(payloadTypeId)) {
            throw new IllegalArgumentException(
                "Unsupported transition payload type: " + payloadTypeId);
        }
        try {
            String encodedPayload = payload instanceof Message protobuf
                ? JsonFormat.printer().omittingInsignificantWhitespace().print(protobuf)
                : mapper.writeValueAsString(payload);
            return new SerializedTransitionPayload(
                payloadTypeId,
                ENCODING,
                encodedPayload);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to encode transition payload type "
                    + payloadTypeId
                    + ": "
                    + e.getMessage(),
                e);
        }
    }

    @Override
    public Object decode(SerializedTransitionPayload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Transition payload must not be null");
        }
        if (!ENCODING.equals(payload.payloadEncoding())) {
            throw new IllegalArgumentException(
                "Unsupported transition payload encoding: " + payload.payloadEncoding());
        }
        if (NULL_TYPE_ID.equals(payload.payloadTypeId())) {
            return null;
        }
        if (ExecutionInputSnapshot.class.getName().equals(payload.payloadTypeId())) {
            return decodeExecutionInputSnapshot(payload);
        }
        if (LIST_TYPE_ID.equals(payload.payloadTypeId())) {
            return decodeList(payload);
        }
        if (SET_TYPE_ID.equals(payload.payloadTypeId())) {
            return decodeSet(payload);
        }
        if (MAP_TYPE_ID.equals(payload.payloadTypeId())) {
            return decodeMap(payload);
        }
        try {
            Class<?> payloadClass = resolvePayloadClass(payload.payloadTypeId());
            if (Message.class.isAssignableFrom(payloadClass)) {
                return decodeProtobufMessage(payload, payloadClass);
            }
            return mapper.readValue(payload.payload(), payloadClass);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to decode transition payload type " + payload.payloadTypeId(),
                e);
        }
    }

    private SerializedTransitionPayload encodeExecutionInputSnapshot(ExecutionInputSnapshot snapshot) {
        try {
            List<?> multiPayload = null;
            if (snapshot.shape() == ExecutionInputShape.MULTI) {
                if (!(snapshot.payload() instanceof List<?> payloadItems)) {
                    throw new IllegalArgumentException(
                        "ExecutionInputShape.MULTI snapshot payload must be a List");
                }
                multiPayload = payloadItems;
            }
            EncodedExecutionInputSnapshot encoded = snapshot.shape() == ExecutionInputShape.MULTI
                ? new EncodedExecutionInputSnapshot(
                    snapshot.shape(),
                    null,
                    multiPayload.stream().map(this::encode).toList())
                : new EncodedExecutionInputSnapshot(snapshot.shape(), encode(snapshot.payload()), List.of());
            return new SerializedTransitionPayload(
                ExecutionInputSnapshot.class.getName(),
                ENCODING,
                mapper.writeValueAsString(encoded));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode transition input snapshot: " + e.getMessage(), e);
        }
    }

    private Object decodeExecutionInputSnapshot(SerializedTransitionPayload payload) {
        EncodedExecutionInputSnapshot encoded;
        try {
            encoded = mapper.readValue(payload.payload(), EncodedExecutionInputSnapshot.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode transition input snapshot envelope", e);
        }
        try {
            Object decodedPayload = encoded.shape() == ExecutionInputShape.MULTI
                ? encoded.items().stream().map(this::decode).toList()
                : decode(encoded.payload());
            return new ExecutionInputSnapshot(encoded.shape(), decodedPayload);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to decode transition input snapshot " + snapshotTypeSummary(encoded),
                e);
        }
    }

    private SerializedTransitionPayload encodeList(List<?> list) {
        try {
            EncodedList encoded = new EncodedList(list.stream().map(this::encode).toList());
            return new SerializedTransitionPayload(LIST_TYPE_ID, ENCODING, mapper.writeValueAsString(encoded));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode transition list payload: " + e.getMessage(), e);
        }
    }

    private SerializedTransitionPayload encodeSet(Set<?> set) {
        try {
            EncodedList encoded = new EncodedList(set.stream().map(this::encode).toList());
            return new SerializedTransitionPayload(SET_TYPE_ID, ENCODING, mapper.writeValueAsString(encoded));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode transition set payload: " + e.getMessage(), e);
        }
    }

    private SerializedTransitionPayload encodeArray(Object array) {
        int length = java.lang.reflect.Array.getLength(array);
        java.util.ArrayList<SerializedTransitionPayload> items = new java.util.ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            items.add(encode(java.lang.reflect.Array.get(array, i)));
        }
        try {
            return new SerializedTransitionPayload(LIST_TYPE_ID, ENCODING, mapper.writeValueAsString(new EncodedList(items)));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode transition array payload: " + e.getMessage(), e);
        }
    }

    private SerializedTransitionPayload encodeMap(Map<?, ?> map) {
        java.util.LinkedHashMap<String, SerializedTransitionPayload> items = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key) || key.isBlank()) {
                throw new IllegalArgumentException("Transition map payload keys must be non-blank strings");
            }
            items.put(key, encode(entry.getValue()));
        }
        try {
            return new SerializedTransitionPayload(MAP_TYPE_ID, ENCODING, mapper.writeValueAsString(new EncodedMap(items)));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode transition map payload: " + e.getMessage(), e);
        }
    }

    private Object decodeList(SerializedTransitionPayload payload) {
        try {
            EncodedList encoded = mapper.readValue(payload.payload(), EncodedList.class);
            return encoded.items().stream().map(this::decode).toList();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode transition list payload", e);
        }
    }

    private Object decodeSet(SerializedTransitionPayload payload) {
        try {
            EncodedList encoded = mapper.readValue(payload.payload(), EncodedList.class);
            java.util.LinkedHashSet<Object> decoded = new java.util.LinkedHashSet<>();
            encoded.items().stream().map(this::decode).forEach(decoded::add);
            return Collections.unmodifiableSet(decoded);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode transition set payload", e);
        }
    }

    private Object decodeMap(SerializedTransitionPayload payload) {
        try {
            EncodedMap encoded = mapper.readValue(payload.payload(), EncodedMap.class);
            java.util.LinkedHashMap<String, Object> decoded = new java.util.LinkedHashMap<>();
            encoded.items().forEach((key, value) -> decoded.put(key, decode(value)));
            return Collections.unmodifiableMap(decoded);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode transition map payload", e);
        }
    }

    private Object decodeProtobufMessage(SerializedTransitionPayload payload, Class<?> payloadClass) {
        try {
            Object builder = payloadClass.getMethod("newBuilder").invoke(null);
            if (!(builder instanceof Message.Builder messageBuilder)) {
                throw new IllegalArgumentException(
                    "Protobuf payload type does not expose a Message.Builder: " + payload.payloadTypeId());
            }
            JsonFormat.parser().ignoringUnknownFields().merge(payload.payload(), messageBuilder);
            return messageBuilder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to decode protobuf transition payload type " + payload.payloadTypeId(),
                e);
        }
    }

    private Class<?> resolvePayloadClass(String payloadTypeId) throws ClassNotFoundException {
        if (!isAllowedPayloadType(payloadTypeId)) {
            throw new IllegalArgumentException("Unsupported transition payload type: " + payloadTypeId);
        }
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        if (contextLoader != null) {
            try {
                return Class.forName(payloadTypeId, false, contextLoader);
            } catch (ClassNotFoundException ignored) {
                // Fall back to the framework class loader below.
            }
        }
        return Class.forName(payloadTypeId, false, JsonTransitionPayloadCodec.class.getClassLoader());
    }

    private boolean isAllowedPayloadType(String payloadTypeId) {
        return payloadTypeId != null
            && (SAFE_JDK_PAYLOAD_TYPES.contains(payloadTypeId)
                || allowedApplicationPayloadPrefixes().stream().anyMatch(payloadTypeId::startsWith));
    }

    private List<String> allowedApplicationPayloadPrefixes() {
        if (orchestratorConfig == null || orchestratorConfig.worker() == null) {
            return DEFAULT_APPLICATION_PAYLOAD_PREFIXES;
        }
        List<String> configured = orchestratorConfig.worker().allowedPayloadPrefixes();
        if (configured == null || configured.isEmpty()) {
            return DEFAULT_APPLICATION_PAYLOAD_PREFIXES;
        }
        List<String> sanitized = configured.stream()
            .filter(prefix -> prefix != null && !prefix.isBlank())
            .toList();
        return sanitized.isEmpty() ? DEFAULT_APPLICATION_PAYLOAD_PREFIXES : sanitized;
    }

    private String snapshotTypeSummary(EncodedExecutionInputSnapshot encoded) {
        if (encoded == null) {
            return "(shape=<unknown>)";
        }
        if (encoded.shape() == ExecutionInputShape.MULTI) {
            List<String> itemTypes = encoded.items().stream()
                .limit(10)
                .map(SerializedTransitionPayload::payloadTypeId)
                .toList();
            String suffix = encoded.items().size() > itemTypes.size() ? ", ..." : "";
            return "(shape=" + encoded.shape()
                + ", itemCount=" + encoded.items().size()
                + ", itemPayloadTypeIds=" + itemTypes + suffix + ")";
        }
        return "(shape=" + encoded.shape()
            + ", payloadTypeId="
            + (encoded.payload() == null ? "<null>" : encoded.payload().payloadTypeId())
            + ")";
    }

    private record EncodedExecutionInputSnapshot(
        ExecutionInputShape shape,
        SerializedTransitionPayload payload,
        List<SerializedTransitionPayload> items) {
        private EncodedExecutionInputSnapshot {
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    private record EncodedList(List<SerializedTransitionPayload> items) {
        private EncodedList {
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    private record EncodedMap(Map<String, SerializedTransitionPayload> items) {
        private EncodedMap {
            items = items == null
                ? Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(items));
        }
    }
}
