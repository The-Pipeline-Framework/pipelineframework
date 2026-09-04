package org.pipelineframework.processor.block;

import java.util.List;
import java.util.Map;

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

    public record Definition(String name, String resource, Map<String, Requirement> requires) {
        public Definition {
            name = requireIdentityComponent(name, "definition.name");
            resource = requireText(resource, "definition.resource");
            requires = requires == null ? Map.of() : Map.copyOf(requires);
            requires.forEach((requirementName, requirement) -> {
                requireIdentityComponent(requirementName, "definition.requires name");
                if (requirement == null) {
                    throw new IllegalArgumentException("definition.requires entry must not be null");
                }
            });
        }
    }

    /** Compile-time connector authority required by one exported Block definition. */
    public record Requirement(String kind) {
        public Requirement {
            kind = requireText(kind, "definition.requires.kind").toUpperCase(java.util.Locale.ROOT);
            if (!"QUERY".equals(kind) && !"COMMAND".equals(kind)) {
                throw new IllegalArgumentException(
                    "definition.requires.kind must be QUERY or COMMAND, got '" + kind + "'");
            }
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
