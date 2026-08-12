package org.pipelineframework.connector;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * Loads provider metadata from artifacts without constructing providers or resolving runtime services.
 */
public final class ConnectorProviderManifestLoader {
    public static final String RESOURCE_PATH = "META-INF/pipeline/connector-providers.json";

    private ConnectorProviderManifestLoader() {
    }

    public static ConnectorProviderManifestCatalog load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "class loader must not be null");
        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_PATH);
            List<URL> ordered = new ArrayList<>();
            while (resources.hasMoreElements()) {
                ordered.add(resources.nextElement());
            }
            ordered.sort(Comparator.comparing(URL::toExternalForm));
            List<ConnectorProviderManifest> manifests = new ArrayList<>();
            for (URL resource : ordered) {
                try {
                    manifests.add(ConnectorProviderManifestReader.read(resource.openStream()));
                } catch (IOException exception) {
                    throw new IllegalStateException("unable to read connector provider metadata at " + resource, exception);
                } catch (IllegalArgumentException exception) {
                    throw new IllegalArgumentException("invalid connector provider metadata at " + resource + ": " + exception.getMessage(), exception);
                }
            }
            return new ConnectorProviderManifestCatalog(manifests);
        } catch (IOException exception) {
            throw new IllegalStateException("unable to discover connector provider metadata", exception);
        }
    }
}
