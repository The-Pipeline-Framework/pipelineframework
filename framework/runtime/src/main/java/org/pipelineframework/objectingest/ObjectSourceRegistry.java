package org.pipelineframework.objectingest;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Plain Java registry for object source providers.
 */
public final class ObjectSourceRegistry {

    private final Map<String, ObjectSourceProvider> providers;

    public ObjectSourceRegistry(Collection<ObjectSourceProvider> providers) {
        Map<String, ObjectSourceProvider> byName = new LinkedHashMap<>();
        if (providers != null) {
            for (ObjectSourceProvider provider : providers) {
                if (provider != null && provider.providerName() != null && !provider.providerName().isBlank()) {
                    String name = provider.providerName().trim().toLowerCase(java.util.Locale.ROOT);
                    if (byName.containsKey(name)) {
                        throw new IllegalArgumentException("Duplicate object source provider: " + name);
                    }
                    byName.put(name, provider);
                }
            }
        }
        this.providers = Map.copyOf(byName);
    }

    public static ObjectSourceRegistry load() {
        try {
            return new ObjectSourceRegistry(ServiceLoader.load(ObjectSourceProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList());
        } catch (ServiceConfigurationError e) {
            throw new IllegalStateException("Failed to load object source providers", e);
        }
    }

    public ObjectSourceProvider require(String providerName) {
        if (providerName == null || providerName.isBlank()) {
            throw new IllegalArgumentException("object source provider must not be blank");
        }
        ObjectSourceProvider provider = providers.get(providerName.trim().toLowerCase(java.util.Locale.ROOT));
        if (provider == null) {
            throw new IllegalStateException("Object source provider not found: " + providerName);
        }
        return provider;
    }

    public Map<String, ObjectSourceProvider> providers() {
        return providers;
    }
}
