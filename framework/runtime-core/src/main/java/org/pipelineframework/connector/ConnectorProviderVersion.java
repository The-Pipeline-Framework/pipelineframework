package org.pipelineframework.connector;

/**
 * Provider binary version. Compatibility is exact-major; minor changes are additive.
 */
public record ConnectorProviderVersion(int major, int minor) {
    public ConnectorProviderVersion {
        if (major < 1) {
            throw new IllegalArgumentException("provider major version must be positive");
        }
        if (minor < 0) {
            throw new IllegalArgumentException("provider minor version must not be negative");
        }
    }

    public boolean isExactMajorCompatibleWith(int expectedMajor) {
        return major == expectedMajor;
    }
}
