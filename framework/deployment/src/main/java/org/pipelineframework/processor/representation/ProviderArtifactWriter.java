package org.pipelineframework.processor.representation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.Writer;
import javax.annotation.processing.Filer;
import javax.tools.JavaFileObject;

import org.pipelineframework.representation.spi.ArtifactDescription;

/** The only component permitted to materialize provider-described source and resource artifacts. */
public final class ProviderArtifactWriter {
    private static final Comparator<ArtifactDescription> ORDER = Comparator
        .comparing((ArtifactDescription artifact) -> artifact.phase().ordinal())
        .thenComparing(ArtifactDescription::providerKey)
        .thenComparing(artifact -> artifact.kind().name())
        .thenComparing(ArtifactDescription::logicalPath)
        .thenComparingInt(ArtifactDescription::providerOrdinal);

    public List<Path> write(Path root, List<ArtifactDescription> descriptions) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("artifact root must not be null");
        }
        Map<String, ArtifactDescription> unique = new LinkedHashMap<>();
        for (ArtifactDescription description : descriptions == null ? List.<ArtifactDescription>of() : descriptions.stream().sorted(ORDER).toList()) {
            validate(description);
            ArtifactDescription previous = unique.putIfAbsent(description.logicalPath(), description);
            if (previous != null) {
                throw new IllegalStateException("Representation artifact conflict at '" + description.logicalPath()
                    + "' between providers '" + previous.providerKey() + "' and '" + description.providerKey() + "'.");
            }
        }
        List<Path> written = new java.util.ArrayList<>();
        for (ArtifactDescription description : unique.values()) {
            Path target = root.resolve(description.logicalPath()).normalize();
            if (!target.startsWith(root.normalize())) {
                throw new IllegalStateException("Representation artifact path escapes generated output: " + description.logicalPath());
            }
            Files.createDirectories(target.getParent());
            Files.writeString(target, description.content(), StandardCharsets.UTF_8);
            written.add(target);
        }
        return List.copyOf(written);
    }

    /** Writes source through the active compiler host, which registers it for the same compilation invocation. */
    public void write(Filer filer, List<ArtifactDescription> descriptions) throws IOException {
        if (filer == null) {
            throw new IllegalArgumentException("filer must not be null");
        }
        Map<String, ArtifactDescription> unique = new LinkedHashMap<>();
        for (ArtifactDescription description : descriptions == null ? List.<ArtifactDescription>of() : descriptions.stream().sorted(ORDER).toList()) {
            validate(description);
            ArtifactDescription previous = unique.putIfAbsent(description.logicalPath(), description);
            if (previous != null) {
                throw new IllegalStateException("Representation artifact conflict at '" + description.logicalPath()
                    + "' between providers '" + previous.providerKey() + "' and '" + description.providerKey() + "'.");
            }
        }
        for (ArtifactDescription description : unique.values()) {
            if (description.kind().name().equals("RESOURCE")) {
                throw new IllegalStateException("Provider resource artifacts require a file-system host output root.");
            }
            String className = description.logicalPath().substring(0, description.logicalPath().length() - ".java".length())
                .replace('/', '.');
            JavaFileObject source = filer.createSourceFile(className);
            try (Writer writer = source.openWriter()) {
                writer.write(description.content());
            }
        }
    }

    private static void validate(ArtifactDescription description) {
        Path path = Path.of(description.logicalPath());
        if (path.isAbsolute() || description.logicalPath().contains("..")) {
            throw new IllegalStateException("Representation artifact path must be relative and normalized: "
                + description.logicalPath());
        }
        if (description.kind().name().equals("JAVA_SOURCE") && !description.logicalPath().endsWith(".java")) {
            throw new IllegalStateException("Java representation artifact must end in .java: " + description.logicalPath());
        }
    }
}
