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
    static final int SCHEMA_VERSION = 2;
    private static final int LEGACY_SCHEMA_VERSION = 1;

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
        PersistedSnapshotV2 snapshot = new PersistedSnapshotV2(
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
            record.attempts().stream()
                .map(attempt -> encodeAttempt(attempt, outputDeclaredType))
                .toList(),
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
        JsonNode root;
        try {
            root = json.readTree(encoded);
        } catch (IOException | RuntimeException failure) {
            throw new CommandEffectStoreException("Failed decoding durable Command effect record", failure);
        }
        int schemaVersion = root.path("schemaVersion").asInt(-1);
        if (!supportsSchemaVersion(schemaVersion)) {
            throw new CommandEffectStoreException(
                "Unsupported durable Command effect schema version " + schemaVersion);
        }
        try {
            return schemaVersion == LEGACY_SCHEMA_VERSION
                ? decodeLegacy(json.treeToValue(root, PersistedSnapshotV1.class))
                : decodeCurrent(json.treeToValue(root, PersistedSnapshotV2.class));
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new CommandEffectStoreException("Invalid durable Command effect record", failure);
        }
    }

    static boolean supportsSchemaVersion(int schemaVersion) {
        return schemaVersion == LEGACY_SCHEMA_VERSION || schemaVersion == SCHEMA_VERSION;
    }

    private DecodedSnapshot decodeCurrent(PersistedSnapshotV2 snapshot) {
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
                snapshot.attempts().stream()
                    .map(attempt -> decodeAttempt(attempt, snapshot.outputDeclaredType()))
                    .toList(),
                snapshot.createdAtEpochMs(),
                snapshot.updatedAtEpochMs());
            return new DecodedSnapshot(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new CommandEffectStoreException("Invalid durable Command effect record", failure);
        }
    }

    private DecodedSnapshot decodeLegacy(PersistedSnapshotV1 snapshot) {
        requireText(snapshot.inputDeclaredType(), "input declared type");
        requireText(snapshot.outputDeclaredType(), "output declared type");
        Object currentOutput = decodeValue(
            requireOptional(snapshot.output(), "output value"), snapshot.outputDeclaredType()).orElse(null);
        List<CommandEffectAttemptRecord> attempts = new java.util.ArrayList<>();
        for (PersistedAttemptSnapshotV1 attempt : snapshot.attempts()) {
            boolean current = attempt.attemptNumber() == snapshot.attempts().size();
            attempts.add(new CommandEffectAttemptRecord(
                attempt.attemptId(),
                snapshot.commandId(),
                attempt.attemptNumber(),
                attempt.executionId(),
                attempt.attemptNumber() == 1 ? CommandAttemptPurpose.INITIAL : CommandAttemptPurpose.RETRY,
                attempt.status(),
                current && attempt.status() == CommandEffectStatus.SUCCEEDED
                    ? Optional.ofNullable(currentOutput)
                    : Optional.empty(),
                requireOptional(attempt.errorClass(), "attempt error class").orElse(null),
                requireOptional(attempt.errorMessage(), "attempt error message").orElse(null),
                requireOptional(attempt.outcome(), "attempt outcome snapshot"),
                Optional.empty(),
                attempt.createdAtEpochMs(),
                attempt.updatedAtEpochMs()));
        }
        CommandEffectRecord record = new CommandEffectRecord(
            snapshot.tenantId(), snapshot.executionId(), snapshot.stepId(), snapshot.command(),
            snapshot.commandId(), Objects.requireNonNull(snapshot.status(), "command effect status must not be null"),
            decodeValue(requireOptional(snapshot.input(), "input value"), snapshot.inputDeclaredType()).orElse(null),
            currentOutput,
            requireOptional(snapshot.errorClass(), "error class").orElse(null),
            requireOptional(snapshot.errorMessage(), "error message").orElse(null),
            requireOptional(snapshot.outcome(), "outcome snapshot"),
            attempts,
            snapshot.createdAtEpochMs(),
            snapshot.updatedAtEpochMs());
        return new DecodedSnapshot(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
    }

    private PersistedAttemptSnapshotV2 encodeAttempt(
        CommandEffectAttemptRecord attempt,
        String outputDeclaredType
    ) {
        return new PersistedAttemptSnapshotV2(
            attempt.attemptId(),
            attempt.occurrenceId(),
            attempt.attemptNumber(),
            attempt.executionId(),
            attempt.purpose(),
            attempt.status(),
            encodeValue(attempt.output().orElse(null), outputDeclaredType),
            Optional.ofNullable(attempt.errorClass()),
            Optional.ofNullable(attempt.errorMessage()),
            attempt.outcome(),
            attempt.reason(),
            attempt.createdAtEpochMs(),
            attempt.updatedAtEpochMs());
    }

    private CommandEffectAttemptRecord decodeAttempt(
        PersistedAttemptSnapshotV2 attempt,
        String outputDeclaredType
    ) {
        return new CommandEffectAttemptRecord(
            attempt.attemptId(),
            attempt.occurrenceId(),
            attempt.attemptNumber(),
            attempt.executionId(),
            attempt.purpose(),
            attempt.status(),
            decodeValue(requireOptional(attempt.output(), "attempt output"), outputDeclaredType),
            requireOptional(attempt.errorClass(), "attempt error class").orElse(null),
            requireOptional(attempt.errorMessage(), "attempt error message").orElse(null),
            requireOptional(attempt.outcome(), "attempt outcome snapshot"),
            requireOptional(attempt.reason(), "attempt reason"),
            attempt.createdAtEpochMs(),
            attempt.updatedAtEpochMs());
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

    private record PersistedAttemptSnapshotV2(
        String attemptId,
        String occurrenceId,
        int attemptNumber,
        String executionId,
        CommandAttemptPurpose purpose,
        CommandEffectStatus status,
        Optional<TypedValueSnapshot> output,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        Optional<String> reason,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
    }

    private record PersistedSnapshotV2(
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
        List<PersistedAttemptSnapshotV2> attempts,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }

    private record PersistedAttemptSnapshotV1(
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
    }

    private record PersistedSnapshotV1(
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
        List<PersistedAttemptSnapshotV1> attempts,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }
}
