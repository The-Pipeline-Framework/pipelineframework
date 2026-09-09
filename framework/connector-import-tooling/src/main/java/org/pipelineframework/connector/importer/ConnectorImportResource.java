package org.pipelineframework.connector.importer;

import java.util.Objects;

/** One deterministic private resource emitted by a release-time Connector importer. */
public record ConnectorImportResource(String path, String content) {
    public ConnectorImportResource {
        path = requireRelativePipelinePath(path);
        content = Objects.requireNonNull(content, "Connector import resource content must not be null");
    }

    private static String requireRelativePipelinePath(String value) {
        String checked = Objects.requireNonNull(value, "Connector import resource path must not be null").trim();
        if (checked.isEmpty() || checked.startsWith("/") || checked.startsWith("\\")
            || checked.contains("\\") || checked.contains("//")) {
            throw new IllegalArgumentException("Connector import resource path must be a relative resource path");
        }
        for (String segment : checked.split("/")) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                throw new IllegalArgumentException("Connector import resource path must not traverse directories");
            }
        }
        if (!checked.startsWith("META-INF/pipeline/")) {
            throw new IllegalArgumentException("Connector import resources must be under META-INF/pipeline");
        }
        return checked;
    }
}
