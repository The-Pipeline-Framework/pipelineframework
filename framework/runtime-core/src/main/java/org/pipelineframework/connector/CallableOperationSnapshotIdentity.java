package org.pipelineframework.connector;

import java.util.Objects;
import java.util.regex.Pattern;

/** Stable identity of a canonical callable-operation snapshot. */
public record CallableOperationSnapshotIdentity(int formatVersion, String sha256) {
    private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

    public CallableOperationSnapshotIdentity {
        if (formatVersion < 1) {
            throw new IllegalArgumentException("callable snapshot format version must be positive");
        }
        Objects.requireNonNull(sha256, "callable snapshot digest must not be null");
        if (!SHA_256.matcher(sha256).matches()) {
            throw new IllegalArgumentException("callable snapshot digest must be a lowercase SHA-256 value");
        }
    }
}
