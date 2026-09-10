package org.pipelineframework.connector.importer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderArtifacts;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifest;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.ConnectorProviderManifestReader;

/** Replaces one import-owned provider and writes its private resources deterministically. */
public final class ConnectorImportWriter {
    private ConnectorImportWriter() {
    }

    public static void write(
        Path root,
        ConnectorProviderArtifactDescriptor replacement,
        Collection<ConnectorImportResource> resources
    ) throws IOException {
        Path checkedRoot = Objects.requireNonNull(root, "Connector import output root must not be null")
            .toAbsolutePath().normalize();
        ConnectorProviderArtifactDescriptor checkedReplacement = Objects.requireNonNull(replacement,
            "Connector import provider must not be null");
        Map<String, String> contentByPath = resources(resources);
        if (contentByPath.containsKey(ConnectorProviderManifestLoader.RESOURCE_PATH)) {
            throw new IllegalArgumentException("Connector import resources must not replace the provider manifest directly");
        }
        Path manifestPath = checkedRoot.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
        ConnectorProviderManifest manifest = replace(manifestPath, checkedReplacement);
        contentByPath.put(ConnectorProviderManifestLoader.RESOURCE_PATH, ConnectorProviderArtifacts.json(manifest));
        writeStaged(checkedRoot, contentByPath);
    }

    private static ConnectorProviderManifest replace(
        Path manifestPath,
        ConnectorProviderArtifactDescriptor replacement
    ) throws IOException {
        List<ConnectorProviderArtifactDescriptor> providers = new ArrayList<>();
        if (Files.isRegularFile(manifestPath)) {
            try (var input = Files.newInputStream(manifestPath)) {
                providers.addAll(ConnectorProviderManifestReader.read(input).providers());
            }
        }
        Set<ConnectorProviderId> identities = new java.util.HashSet<>();
        for (ConnectorProviderArtifactDescriptor provider : providers) {
            if (!identities.add(provider.provider().id())) {
                throw new IllegalArgumentException(
                    "existing Connector provider manifest contains duplicate provider '"
                        + provider.provider().id().value() + "'");
            }
        }
        providers.removeIf(provider -> provider.provider().id().equals(replacement.provider().id()));
        providers.add(replacement);
        providers.sort(Comparator.comparing(provider -> provider.provider().id()));
        return new ConnectorProviderManifest(ConnectorProviderManifest.CURRENT_SCHEMA_VERSION, providers);
    }

    private static Map<String, String> resources(Collection<ConnectorImportResource> resources) {
        Objects.requireNonNull(resources, "Connector import resources must not be null");
        Map<String, String> result = new LinkedHashMap<>();
        resources.stream().map(resource -> Objects.requireNonNull(resource,
            "Connector import resource must not be null")).sorted(Comparator.comparing(ConnectorImportResource::path))
            .forEach(resource -> {
                if (result.putIfAbsent(resource.path(), resource.content()) != null) {
                    throw new IllegalArgumentException("duplicate Connector import resource '" + resource.path() + "'");
                }
            });
        return result;
    }

    private static void writeStaged(Path root, Map<String, String> contentByPath) throws IOException {
        Map<Path, Path> staged = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, String> entry : contentByPath.entrySet()) {
                Path target = root.resolve(entry.getKey()).normalize();
                if (!target.startsWith(root)) {
                    throw new IllegalArgumentException("Connector import resource escapes its output root");
                }
                Files.createDirectories(target.getParent());
                Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
                Files.writeString(temporary, entry.getValue(), StandardCharsets.UTF_8);
                staged.put(target, temporary);
            }
            Path manifest = root.resolve(ConnectorProviderManifestLoader.RESOURCE_PATH);
            for (Map.Entry<Path, Path> entry : staged.entrySet()) {
                if (!entry.getKey().equals(manifest)) {
                    move(entry.getValue(), entry.getKey());
                }
            }
            Map.Entry<Path, Path> manifestEntry = staged.entrySet().stream()
                .filter(entry -> entry.getKey().equals(manifest)).findFirst().orElseThrow();
            move(manifestEntry.getValue(), manifestEntry.getKey());
        } finally {
            for (Path temporary : staged.values()) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static void move(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
