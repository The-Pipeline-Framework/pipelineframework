package org.pipelineframework.query;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Versioned durable representation of immutable Query capture events. */
final class QueryCaptureEventCodec {
    static final int SCHEMA_VERSION = 1;
    static final String REDACTED_INPUT_PREFIX = "sha256:";

    private static final String NO_VALUE = "";
    private static final long NO_NUMBER = -1L;

    private final ObjectMapper json;
    private final ClassLoader classLoader;
    private final QueryCapturePayloadCodec payloadCodec;

    QueryCaptureEventCodec() {
        this(PipelineJson.mapper(), Thread.currentThread().getContextClassLoader());
    }

    QueryCaptureEventCodec(ObjectMapper json, ClassLoader classLoader) {
        this.json = Objects.requireNonNull(json, "query capture JSON mapper must not be null");
        this.classLoader = classLoader == null ? QueryCaptureEventCodec.class.getClassLoader() : classLoader;
        this.payloadCodec = new QueryCapturePayloadCodec(json);
    }

    String encode(Event event) {
        validate(event);
        try {
            return json.writeValueAsString(event);
        } catch (Exception failure) {
            throw new QueryCaptureStoreException("Failed encoding durable Query capture event", failure);
        }
    }

    Event decode(String encoded) {
        requireText(encoded, "encoded Query capture event");
        try {
            Event event = json.readValue(encoded, Event.class);
            validate(event);
            return event;
        } catch (QueryCaptureStoreException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new QueryCaptureStoreException("Failed decoding durable Query capture event", failure);
        }
    }

    Event unary(QueryCaptureRecord record) {
        Objects.requireNonNull(record, "Query capture record must not be null");
        Kind kind = record.status() == QueryCaptureStatus.FOUND ? Kind.FOUND : Kind.NOT_FOUND;
        String runtimeType = kind == Kind.FOUND
            ? runtimeType(record.outputJson(), record.outputType())
            : record.outputType();
        return new Event(
            SCHEMA_VERSION, kind, record.tenantId(), record.executionId(), record.stepIndex(),
            record.queryId(), record.queryVersion(), record.captureKey(), digest(record.inputJson()),
            record.outputJson(), record.outputType(), runtimeType, encoding(record.outputType()),
            record.capturedAt().toEpochMilli(), record.status(), record.outcomeCode(),
            NO_NUMBER, NO_VALUE, NO_NUMBER, NO_NUMBER, NO_NUMBER);
    }

    Event streamOpen(StreamingQueryCaptureRequest request, long generation, String ownerToken, long leaseExpiresAt) {
        return new Event(
            SCHEMA_VERSION, Kind.STREAM_OPEN, request.tenantId(), request.executionId(), request.stepIndex(),
            request.queryId(), request.queryVersion(), request.captureKey(), digest(request.inputJson()),
            NO_VALUE, request.outputType(), request.outputType(), encoding(request.outputType()),
            Instant.now().toEpochMilli(), QueryCaptureStatus.FOUND, "found", generation, ownerToken,
            leaseExpiresAt, NO_NUMBER, NO_NUMBER);
    }

    Event streamItem(Event authority, StreamingQueryCaptureItem item, long leaseExpiresAt) {
        return new Event(
            authority.schemaVersion(), Kind.STREAM_ITEM, authority.tenantId(), authority.executionId(),
            authority.stepIndex(), authority.queryId(), authority.queryVersion(), authority.captureKey(),
            authority.inputDigest(), item.outputJson(), authority.outputType(),
            runtimeType(item.outputJson(), authority.outputType()), authority.encoding(),
            Instant.now().toEpochMilli(), authority.status(), authority.outcomeCode(),
            authority.generation(), authority.ownerToken(), leaseExpiresAt, item.ordinal(), NO_NUMBER);
    }

    Event streamTerminal(Event authority, Kind kind, long itemCount) {
        return authority.withStreamState(kind, NO_VALUE, NO_NUMBER, NO_NUMBER, itemCount);
    }

    Event tombstone(Event authority) {
        return authority.withStreamState(Kind.TOMBSTONE, NO_VALUE, NO_NUMBER, NO_NUMBER, NO_NUMBER);
    }

    QueryCaptureRecord toRecord(Event event) {
        if (event.kind() != Kind.FOUND && event.kind() != Kind.NOT_FOUND) {
            throw new QueryCaptureStoreException("Durable Query capture is not a unary observation");
        }
        return new QueryCaptureRecord(
            event.tenantId(), event.executionId(), event.stepIndex(), event.queryId(), event.queryVersion(),
            event.captureKey(), REDACTED_INPUT_PREFIX + event.inputDigest(), event.outputJson(), event.outputType(),
            Instant.ofEpochMilli(event.capturedAtEpochMs()), event.status(), event.outcomeCode());
    }

    StreamingQueryCaptureItem toItem(Event event) {
        if (event.kind() != Kind.STREAM_ITEM) {
            throw new QueryCaptureStoreException("Durable Query capture event is not a streaming item");
        }
        return new StreamingQueryCaptureItem(event.itemOrdinal(), event.outputJson(), event.outputType());
    }

    private void validate(Event event) {
        if (event == null) {
            throw new QueryCaptureStoreException("Durable Query capture event must not be null");
        }
        if (event.schemaVersion() != SCHEMA_VERSION) {
            throw new QueryCaptureStoreException(
                "Unsupported durable Query capture schema version " + event.schemaVersion());
        }
        Objects.requireNonNull(event.kind(), "durable Query capture kind must not be null");
        requireText(event.tenantId(), "tenantId");
        requireText(event.executionId(), "executionId");
        requireText(event.queryId(), "queryId");
        requireText(event.queryVersion(), "queryVersion");
        requireText(event.captureKey(), "captureKey");
        requireText(event.inputDigest(), "input digest");
        if (event.stepIndex() < 0) {
            throw new QueryCaptureStoreException("Durable Query capture step index must be non-negative");
        }
        if (event.kind() == Kind.FOUND || event.kind() == Kind.STREAM_ITEM) {
            requireText(event.outputJson(), "output JSON");
            Class<?> outputType = resolve(event.outputType());
            Class<?> runtimeType = resolve(event.runtimeType());
            if (!outputType.isAssignableFrom(runtimeType)) {
                throw new QueryCaptureStoreException(
                    "Durable Query runtime type " + runtimeType.getName()
                        + " is incompatible with declared type " + outputType.getName());
            }
            validateJson(event.outputJson());
        }
        if (event.kind().isStreaming()) {
            requireText(event.outputType(), "streaming output type");
            String expectedEncoding = encoding(event.outputType());
            if (!expectedEncoding.equals(event.encoding())) {
                throw new QueryCaptureStoreException(
                    "Durable Query capture encoding " + event.encoding()
                        + " does not match output type " + event.outputType());
            }
            requireText(event.ownerToken(), "streaming owner token");
            if (event.generation() < 0) {
                throw new QueryCaptureStoreException("Streaming Query generation must be non-negative");
            }
        }
    }

    private void validateJson(String payload) {
        try {
            if (json.readTree(payload) == null) {
                throw new QueryCaptureStoreException("Durable Query output JSON must not be empty");
            }
        } catch (QueryCaptureStoreException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new QueryCaptureStoreException("Durable Query output JSON is corrupt", failure);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private String runtimeType(String payload, String declaredType) {
        Class<?> type = resolve(declaredType);
        Object decoded = payloadCodec.decode(payload, (Class) type);
        return decoded.getClass().getName();
    }

    private String encoding(String typeName) {
        if (typeName == null || typeName.isBlank()) {
            return NO_VALUE;
        }
        Class<?> type = resolve(typeName);
        if (Message.class.isAssignableFrom(type)) {
            return "protobuf-json";
        }
        return type.isSealed() ? "union-json" : "pipeline-json";
    }

    private Class<?> resolve(String typeName) {
        requireText(typeName, "output type");
        try {
            return classLoader.loadClass(typeName);
        } catch (ClassNotFoundException original) {
            String[] segments = typeName.split("\\.");
            for (int packageSegments = segments.length - 2; packageSegments >= 1; packageSegments--) {
                String packageName = String.join(".", Arrays.copyOfRange(segments, 0, packageSegments));
                String nestedName = String.join("$", Arrays.copyOfRange(segments, packageSegments, segments.length));
                try {
                    return classLoader.loadClass(packageName + "." + nestedName);
                } catch (ClassNotFoundException ignored) {
                    // Try the next possible package/type boundary.
                }
            }
            throw new QueryCaptureStoreException(
                "Durable Query output type is not available: " + typeName, original);
        }
    }

    private static String digest(String inputJson) {
        String input = inputJson == null ? NO_VALUE : inputJson;
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception failure) {
            throw new QueryCaptureStoreException("SHA-256 is unavailable for Query capture", failure);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new QueryCaptureStoreException("Durable Query capture " + field + " must not be blank");
        }
    }

    enum Kind {
        FOUND,
        NOT_FOUND,
        STREAM_OPEN,
        STREAM_ITEM,
        STREAM_COMMITTED,
        STREAM_ABORTED,
        TOMBSTONE;

        boolean isStreaming() {
            return this == STREAM_OPEN || this == STREAM_ITEM || this == STREAM_COMMITTED
                || this == STREAM_ABORTED;
        }
    }

    record Event(
        int schemaVersion,
        Kind kind,
        String tenantId,
        String executionId,
        int stepIndex,
        String queryId,
        String queryVersion,
        String captureKey,
        String inputDigest,
        String outputJson,
        String outputType,
        String runtimeType,
        String encoding,
        long capturedAtEpochMs,
        QueryCaptureStatus status,
        String outcomeCode,
        long generation,
        String ownerToken,
        long leaseExpiresAtEpochMs,
        long itemOrdinal,
        long itemCount
    ) {
        Event withStreamState(Kind nextKind, String nextOutputJson, long nextLease, long ordinal, long count) {
            return new Event(
                schemaVersion, nextKind, tenantId, executionId, stepIndex, queryId, queryVersion,
                captureKey, inputDigest, nextOutputJson, outputType, runtimeType, encoding,
                Instant.now().toEpochMilli(), status, outcomeCode, generation, ownerToken,
                nextLease, ordinal, count);
        }
    }
}
