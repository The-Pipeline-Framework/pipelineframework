package org.pipelineframework.connector;

/**
 * Scalar configuration shapes supported by the v1 record binder.
 */
public enum ConnectorConfigValueType {
    STRING,
    BOOLEAN,
    INTEGER,
    DECIMAL,
    ENUM,
    DURATION,
    CONNECTION_REF,
    SECRET_REF
}
