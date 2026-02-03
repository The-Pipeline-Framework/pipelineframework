package org.pipelineframework.processor.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
     * Locate the runtime mapping configuration file for the given module.
     *
     * @param moduleDir the module directory to search from
     * @return the resolved config path if found
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
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
            throw new IllegalStateException("Multiple runtime mapping files found: " + names);
        }

        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

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

    private void scanDirectory(Path directory, List<Path> matches) {
        if (!Files.isDirectory(directory)) {
            return;
        }

        try (var stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                .forEach(path -> {
                    String filename = path.getFileName().toString();
                    if (EXACT_FILENAMES.contains(filename)) {
                        matches.add(path);
                    }
                });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan runtime mapping directory: " + directory, e);
        }
    }
}
