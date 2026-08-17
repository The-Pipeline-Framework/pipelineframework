package org.pipelineframework.connector;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Immutable, deterministic model-visible projection of already-authorized operations. */
public record CallableOperationSnapshot(
    CallableOperationSnapshotIdentity identity,
    List<CallableOperationDefinition> operations
) {
    public static final int FORMAT_VERSION = 1;

    public CallableOperationSnapshot {
        identity = Objects.requireNonNull(identity, "callable snapshot identity must not be null");
        operations = sorted(operations);
        CallableOperationSnapshotIdentity expected = identityOf(operations);
        if (!expected.equals(identity)) {
            throw new IllegalArgumentException("callable snapshot identity does not match its operation definitions");
        }
    }

    public static CallableOperationSnapshot of(Collection<CallableOperationDefinition> operations) {
        List<CallableOperationDefinition> ordered = sorted(operations);
        return new CallableOperationSnapshot(identityOf(ordered), ordered);
    }

    private static List<CallableOperationDefinition> sorted(Collection<CallableOperationDefinition> operations) {
        Objects.requireNonNull(operations, "callable operations must not be null");
        List<CallableOperationDefinition> ordered = new ArrayList<>();
        for (CallableOperationDefinition operation : operations) {
            ordered.add(Objects.requireNonNull(operation, "callable operation must not be null"));
        }
        ordered.sort(CallableOperationDefinition::compareTo);
        for (int index = 1; index < ordered.size(); index++) {
            if (ordered.get(index - 1).identity().equals(ordered.get(index).identity())) {
                throw new IllegalArgumentException("duplicate callable operation identity: " + ordered.get(index).identity());
            }
        }
        return List.copyOf(ordered);
    }

    private static CallableOperationSnapshotIdentity identityOf(List<CallableOperationDefinition> operations) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            CanonicalDigest canonical = new CanonicalDigest(digest);
            canonical.value("tpf:callable-operation-snapshot");
            canonical.value(Integer.toString(FORMAT_VERSION));
            canonical.value(Integer.toString(operations.size()));
            operations.forEach(canonical::operation);
            return new CallableOperationSnapshotIdentity(FORMAT_VERSION, HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static final class CanonicalDigest {
        private final MessageDigest digest;

        private CanonicalDigest(MessageDigest digest) {
            this.digest = digest;
        }

        private void operation(CallableOperationDefinition operation) {
            ConnectorOperationIdentity identity = operation.identity();
            value(identity.providerId().value());
            value(identity.operationId());
            value(identity.kind().value());
            value(Integer.toString(identity.majorVersion()));
            value(operation.description());
            value(operation.typeContract().inputType());
            optional(operation.typeContract().outputType(), this::value);
            optional(operation.commandCapabilities(), capabilities -> {
                value(capabilities.executionPosture().name());
                value(capabilities.maximumMachineConfirmation().name());
                value(Boolean.toString(capabilities.userConfirmationSupported()));
            });
            optional(operation.queryCapabilities(), capabilities -> {
                value(capabilities.cacheability().name());
                optional(capabilities.maximumCacheAge(), duration -> value(duration.toString()));
                optional(capabilities.maximumNegativeCacheTtl(), duration -> value(duration.toString()));
            });
        }

        private <T> void optional(Optional<T> optional, java.util.function.Consumer<T> consumer) {
            value(optional.isPresent() ? "present" : "absent");
            optional.ifPresent(consumer);
        }

        private void value(String value) {
            byte[] bytes = Objects.requireNonNull(value, "canonical digest value must not be null")
                .getBytes(StandardCharsets.UTF_8);
            digest.update((byte) (bytes.length >>> 24));
            digest.update((byte) (bytes.length >>> 16));
            digest.update((byte) (bytes.length >>> 8));
            digest.update((byte) bytes.length);
            digest.update(bytes);
        }
    }
}
