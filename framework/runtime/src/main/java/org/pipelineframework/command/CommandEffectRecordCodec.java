package org.pipelineframework.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Versioned, type-preserving durable representation of a Command effect record. */
final class CommandEffectRecordCodec {
    static final int SCHEMA_VERSION = 1;

    private static final String JSON_ENCODING = "json";
    private static final String PROTOBUF_JSON_ENCODING = "protobuf-json";
    private static final JsonFormat.Printer PROTOBUF_PRINTER =
        JsonFormat.printer().omittingInsignificantWhitespace();

    private final ObjectMapper json;
    private final ClassLoader classLoader;

    CommandEffectRecordCodec() {
        this(PipelineJson.mapper(), Thread.currentThread().getContextClassLoader());
    }

    CommandEffectRecordCodec(ObjectMapper json, ClassLoader classLoader) {
        this.json = Objects.requireNonNull(json, "json mapper must not be null");
        this.classLoader = classLoader == null ? CommandEffectRecordCodec.class.getClassLoader() : classLoader;
    }

    String encode(CommandEffectRecord record, String inputDeclaredType, String outputDeclaredType) {
        Objects.requireNonNull(record, "command effect record must not be null");
        requireText(inputDeclaredType, "input declared type");
        requireText(outputDeclaredType, "output declared type");
        PersistedSnapshot snapshot = new PersistedSnapshot(
            SCHEMA_VERSION,
            record.tenantId(),
            record.executionId(),
            record.stepId(),
            record.command(),
            record.commandId(),
            record.status(),
            encodeValue(record.input(), inputDeclaredType),
            encodeValue(record.output(), outputDeclaredType),
            Optional.ofNullable(record.errorClass()),
            Optional.ofNullable(record.errorMessage()),
            record.outcome(),
            record.attempts().stream().map(PersistedAttemptSnapshot::from).toList(),
            record.createdAtEpochMs(),
            record.updatedAtEpochMs(),
            inputDeclaredType,
            outputDeclaredType);
        try {
            return json.writeValueAsString(snapshot);
        } catch (IOException failure) {
            throw new CommandEffectStoreException(
                "Failed encoding Command effect " + record.commandId(), failure);
        }
    }

    DecodedSnapshot decode(String encoded) {
        requireText(encoded, "encoded command effect record");
        PersistedSnapshot snapshot;
        try {
            snapshot = json.readValue(encoded, PersistedSnapshot.class);
        } catch (IOException | RuntimeException failure) {
            throw new CommandEffectStoreException("Failed decoding durable Command effect record", failure);
        }
        if (snapshot.schemaVersion() != SCHEMA_VERSION) {
            throw new CommandEffectStoreException(
                "Unsupported durable Command effect schema version " + snapshot.schemaVersion());
        }
        requireText(snapshot.inputDeclaredType(), "input declared type");
        requireText(snapshot.outputDeclaredType(), "output declared type");
        try {
            CommandEffectRecord record = new CommandEffectRecord(
                snapshot.tenantId(),
                snapshot.executionId(),
                snapshot.stepId(),
                snapshot.command(),
                snapshot.commandId(),
                Objects.requireNonNull(snapshot.status(), "command effect status must not be null"),
                decodeValue(requireOptional(snapshot.input(), "input value"), snapshot.inputDeclaredType())
                    .orElse(null),
                decodeValue(requireOptional(snapshot.output(), "output value"), snapshot.outputDeclaredType())
                    .orElse(null),
                requireOptional(snapshot.errorClass(), "error class").orElse(null),
                requireOptional(snapshot.errorMessage(), "error message").orElse(null),
                requireOptional(snapshot.outcome(), "outcome snapshot"),
                snapshot.attempts().stream().map(PersistedAttemptSnapshot::toRecord).toList(),
                snapshot.createdAtEpochMs(),
                snapshot.updatedAtEpochMs());
            return new DecodedSnapshot(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new CommandEffectStoreException("Invalid durable Command effect record", failure);
        }
    }

    private Optional<TypedValueSnapshot> encodeValue(Object value, String declaredType) {
        if (value == null) {
            return Optional.empty();
        }
        String runtimeType = value.getClass().getName();
        verifyDeclaredType(declaredType, value.getClass());
        try {
            if (value instanceof MessageOrBuilder protobuf) {
                return Optional.of(new TypedValueSnapshot(
                    runtimeType,
                    PROTOBUF_JSON_ENCODING,
                    json.readTree(PROTOBUF_PRINTER.print(protobuf))));
            }
            return Optional.of(new TypedValueSnapshot(runtimeType, JSON_ENCODING, json.valueToTree(value)));
        } catch (Exception failure) {
            throw new CommandEffectStoreException("Failed encoding durable value of type " + runtimeType, failure);
        }
    }

    private Optional<Object> decodeValue(Optional<TypedValueSnapshot> encoded, String declaredType) {
        if (encoded.isEmpty()) {
            return Optional.empty();
        }
        TypedValueSnapshot snapshot = encoded.orElseThrow();
        requireText(snapshot.runtimeType(), "runtime type");
        requireText(snapshot.encoding(), "value encoding");
        if (snapshot.value() == null) {
            throw new CommandEffectStoreException("Durable typed value is missing its JSON value");
        }
        Class<?> runtimeType = resolve(snapshot.runtimeType());
        verifyDeclaredType(declaredType, runtimeType);
        try {
            return Optional.of(switch (snapshot.encoding()) {
                case JSON_ENCODING -> json.treeToValue(snapshot.value(), runtimeType);
                case PROTOBUF_JSON_ENCODING -> decodeProtobuf(snapshot.value(), runtimeType);
                default -> throw new CommandEffectStoreException(
                    "Unsupported durable Command value encoding " + snapshot.encoding());
            });
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new CommandEffectStoreException(
                "Failed decoding durable value as " + runtimeType.getName(), failure);
        }
    }

    private Object decodeProtobuf(JsonNode value, Class<?> runtimeType) throws Exception {
        if (!Message.class.isAssignableFrom(runtimeType)) {
            throw new CommandEffectStoreException(
                "Durable protobuf value declares non-protobuf type " + runtimeType.getName());
        }
        Message.Builder builder = (Message.Builder) runtimeType.getMethod("newBuilder").invoke(null);
        JsonFormat.parser().merge(json.writeValueAsString(value), builder);
        return builder.build();
    }

    private void verifyDeclaredType(String declaredType, Class<?> runtimeType) {
        Optional<Class<?>> resolved = tryResolve(declaredType);
        if (resolved.isPresent() && !resolved.orElseThrow().isAssignableFrom(runtimeType)) {
            throw new CommandEffectStoreException(
                "Durable value type " + runtimeType.getName()
                    + " is incompatible with declared Command type " + declaredType);
        }
    }

    private Optional<Class<?>> tryResolve(String className) {
        if (className == null || className.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(resolveClass(className));
        } catch (ClassNotFoundException ignored) {
            return Optional.empty();
        }
    }

    private Class<?> resolve(String className) {
        try {
            return resolveClass(className);
        } catch (ClassNotFoundException failure) {
            throw new CommandEffectStoreException(
                "Durable Command value type is not available: " + className, failure);
        }
    }

    private Class<?> resolveClass(String className) throws ClassNotFoundException {
        try {
            return classLoader.loadClass(className);
        } catch (ClassNotFoundException original) {
            String[] segments = className.split("\\.");
            for (int packageSegments = segments.length - 2; packageSegments >= 1; packageSegments--) {
                String packageName = String.join(".", Arrays.copyOfRange(segments, 0, packageSegments));
                String nestedTypeName = String.join("$", Arrays.copyOfRange(segments, packageSegments, segments.length));
                try {
                    return classLoader.loadClass(packageName + "." + nestedTypeName);
                } catch (ClassNotFoundException ignored) {
                    // Try the next possible package/type boundary.
                }
            }
            throw original;
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new CommandEffectStoreException(field + " must not be blank");
        }
    }

    private static <T> Optional<T> requireOptional(Optional<T> value, String field) {
        if (value == null) {
            throw new CommandEffectStoreException("Durable Command effect is missing " + field + " metadata");
        }
        return value;
    }

    record DecodedSnapshot(
        CommandEffectRecord record,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }

    private record TypedValueSnapshot(
        String runtimeType,
        String encoding,
        JsonNode value
    ) {
    }

    private record PersistedAttemptSnapshot(
        String attemptId,
        int attemptNumber,
        String executionId,
        CommandEffectStatus status,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
        private static PersistedAttemptSnapshot from(CommandEffectAttemptRecord attempt) {
            return new PersistedAttemptSnapshot(
                attempt.attemptId(),
                attempt.attemptNumber(),
                attempt.executionId(),
                attempt.status(),
                Optional.ofNullable(attempt.errorClass()),
                Optional.ofNullable(attempt.errorMessage()),
                attempt.outcome(),
                attempt.createdAtEpochMs(),
                attempt.updatedAtEpochMs());
        }

        private CommandEffectAttemptRecord toRecord() {
            return new CommandEffectAttemptRecord(
                attemptId,
                attemptNumber,
                executionId,
                status,
                requireOptional(errorClass, "attempt error class").orElse(null),
                requireOptional(errorMessage, "attempt error message").orElse(null),
                requireOptional(outcome, "attempt outcome snapshot"),
                createdAtEpochMs,
                updatedAtEpochMs);
        }
    }

    private record PersistedSnapshot(
        int schemaVersion,
        String tenantId,
        String executionId,
        String stepId,
        String command,
        String commandId,
        CommandEffectStatus status,
        Optional<TypedValueSnapshot> input,
        Optional<TypedValueSnapshot> output,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        List<PersistedAttemptSnapshot> attempts,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }
}
