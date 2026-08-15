package org.pipelineframework.protocol;

import java.util.Objects;
import java.util.regex.Pattern;

import org.pipelineframework.connector.ConnectorProviderId;

/** Stable qualified identity for framework- or extension-contributed protocol vocabulary. */
public record ProtocolTypeIdentity(ConnectorProviderId namespace, String typeName)
    implements Comparable<ProtocolTypeIdentity> {
    private static final Pattern TYPE_NAME = Pattern.compile("[A-Z][A-Za-z0-9_]*");

    public ProtocolTypeIdentity {
        namespace = Objects.requireNonNull(namespace, "protocol type namespace must not be null");
        Objects.requireNonNull(typeName, "protocol type name must not be null");
        if (!TYPE_NAME.matcher(typeName).matches()) {
            throw new IllegalArgumentException("protocol type name must be an upper-camel identifier: " + typeName);
        }
    }

    public static ProtocolTypeIdentity of(String qualifiedName) {
        Objects.requireNonNull(qualifiedName, "qualified protocol type name must not be null");
        int separator = qualifiedName.lastIndexOf('.');
        if (separator < 1 || separator == qualifiedName.length() - 1) {
            throw new IllegalArgumentException("qualified protocol type name must be namespace.TypeName: " + qualifiedName);
        }
        return new ProtocolTypeIdentity(
            ConnectorProviderId.of(qualifiedName.substring(0, separator)), qualifiedName.substring(separator + 1));
    }

    public String qualifiedName() {
        return namespace.value() + "." + typeName;
    }

    @Override
    public int compareTo(ProtocolTypeIdentity other) {
        return qualifiedName().compareTo(Objects.requireNonNull(other, "other must not be null").qualifiedName());
    }

    @Override
    public String toString() {
        return qualifiedName();
    }
}
