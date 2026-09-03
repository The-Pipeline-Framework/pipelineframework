package org.pipelineframework.processor.block;

import java.util.List;

/** Static compiler manifest published by a block artifact. */
public record BlockPackageManifest(
    int schemaVersion,
    String namespace,
    Artifact artifact,
    List<Definition> definitions
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;

    public BlockPackageManifest {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported block manifest schemaVersion " + schemaVersion);
        }
        namespace = requireIdentityComponent(namespace, "namespace");
        if (artifact == null) {
            throw new IllegalArgumentException("artifact must not be null");
        }
        definitions = definitions == null ? List.of() : List.copyOf(definitions);
        if (definitions.isEmpty()) {
            throw new IllegalArgumentException("definitions must not be empty");
        }
    }

    public record Artifact(String groupId, String artifactId, String version) {
        public Artifact {
            groupId = requireText(groupId, "artifact.groupId");
            artifactId = requireText(artifactId, "artifact.artifactId");
            version = requireText(version, "artifact.version");
        }
    }

    public record Definition(String name, String resource) {
        public Definition {
            name = requireIdentityComponent(name, "definition.name");
            resource = requireText(resource, "definition.resource");
        }
    }

    private static String requireIdentityComponent(String value, String name) {
        String normalized = requireText(value, name);
        if (normalized.contains("/")) {
            throw new IllegalArgumentException(name + " must not contain '/'");
        }
        return normalized;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.strip();
    }
}
