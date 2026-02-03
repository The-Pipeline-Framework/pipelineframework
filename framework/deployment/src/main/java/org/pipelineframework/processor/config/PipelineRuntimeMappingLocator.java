package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * Locates pipeline.runtime.yaml configuration files starting from a module directory.
 */
public class PipelineRuntimeMappingLocator {

    private static final List<String> EXACT_FILENAMES = List.of(
        "pipeline.runtime.yaml",
        "pipeline.runtime.yml"
    );

    /**
     * Creates a new PipelineRuntimeMappingLocator.
     */
    public PipelineRuntimeMappingLocator() {
    }

    /**
     * Locate the pipeline runtime mapping file starting from the given module directory.
     *
     * @param moduleDir the starting module directory from which to search upward for a project root and mapping file
     * @return the located mapping file path, or empty if no mapping file is found
     */
    public Optional<Path> locate(Path moduleDir) {
        Path projectRoot = findNearestParentPom(moduleDir);
        if (projectRoot == null) {
            return Optional.empty();
        }

        List<Path> matches = new ArrayList<>();
        scanDirectory(projectRoot, matches);
        scanDirectory(projectRoot.resolve("config"), matches);

        if (matches.size() > 1) {
            String names = matches.stream()
                .map(Path::toString)
                .sorted()
                .collect(Collectors.joining(", "));
            throw new IllegalStateException("Multiple runtime mapping files found: " + names);
        }

        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    /**
     * Locate the nearest ancestor directory of {@code moduleDir} that contains a {@code pom.xml}, preferring a POM whose `<packaging>` is `pom`.
     *
     * @param moduleDir the starting directory to search upward from
     * @return the nearest directory containing a {@code pom.xml} with `<packaging>` equal to `pom` if present; otherwise the nearest directory containing any {@code pom.xml}; or {@code null} if no {@code pom.xml} is found
     */
    private Path findNearestParentPom(Path moduleDir) {
        Path current = moduleDir;
        Path fallback = null;
        while (current != null) {
            Path pomPath = current.resolve("pom.xml");
            if (Files.isRegularFile(pomPath)) {
                if (fallback == null) {
                    fallback = current;
                }
                if (isPomPackagingPom(pomPath)) {
                    return current;
                }
            }
            current = current.getParent();
        }
        return fallback;
    }

    /**
     * Checks whether the given pom.xml declares a packaging type of "pom".
     *
     * @param pomPath the path to the pom.xml file to inspect
     * @return `true` if the pom's `<packaging>` element equals "pom" (case-insensitive), `false` otherwise or if the file cannot be parsed
     */
    private boolean isPomPackagingPom(Path pomPath) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);

            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document;
            try (InputStream input = Files.newInputStream(pomPath)) {
                document = builder.parse(input);
            }
            NodeList packagingNodes = document.getElementsByTagName("packaging");
            if (packagingNodes.getLength() == 0) {
                return false;
            }
            String packaging = packagingNodes.item(0).getTextContent();
            return packaging != null && packaging.trim().equalsIgnoreCase("pom");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Scans the given directory for files named "pipeline.runtime.yaml" or "pipeline.runtime.yml" and adds any matches to the provided list.
     *
     * If the supplied path is not a directory the method returns without modification.
     *
     * @param directory the directory to scan
     * @param matches a mutable list that will be appended with any matching file paths found in the directory
     * @throws IllegalStateException if an I/O error occurs while listing the directory
     */
    private void scanDirectory(Path directory, List<Path> matches) {
        if (!Files.isDirectory(directory)) {
            return;
        }

        try (var stream = Files.list(directory)) {
            List<Path> found = stream
                .filter(Files::isRegularFile)
                .filter(path -> EXACT_FILENAMES.contains(path.getFileName().toString()))
                .collect(Collectors.toList());
            matches.addAll(found);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan runtime mapping directory: " + directory, e);
        }
    }
}
