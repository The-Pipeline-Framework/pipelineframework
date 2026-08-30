package org.pipelineframework.connector;

/** Configuration shapes supported by the v1 record binder. */
public enum ConnectorConfigValueType {
    STRING,
    BOOLEAN,
    INTEGER,
    DECIMAL,
    ENUM,
    DURATION,
    CONNECTION_REF,
    /** @deprecated Use a tenant-aware {@link ConnectionRef} for authenticated connector access. */
    @Deprecated(forRemoval = true)
    SECRET_REF,
    MAP
}
