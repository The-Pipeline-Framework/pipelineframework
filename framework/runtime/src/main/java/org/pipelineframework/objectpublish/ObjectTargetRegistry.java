package org.pipelineframework.objectpublish;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Registry of object publish target providers.
 */
public final class ObjectTargetRegistry {

    private final Map<String, ObjectTargetProvider> providers;

    public ObjectTargetRegistry(Collection<ObjectTargetProvider> providers) {
        Map<String, ObjectTargetProvider> byName = new LinkedHashMap<>();
        if (providers != null) {
            for (ObjectTargetProvider provider : providers) {
                String name = normalize(provider.providerName());
                if (byName.putIfAbsent(name, provider) != null) {
                    throw new IllegalArgumentException("Duplicate object target provider: " + name);
                }
            }
        }
        this.providers = Map.copyOf(byName);
    }

    public static ObjectTargetRegistry load() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return new ObjectTargetRegistry(ServiceLoader.load(ObjectTargetProvider.class, loader)
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList());
    }

    public ObjectTargetProvider require(String providerName) {
        String normalized = normalize(providerName);
        ObjectTargetProvider provider = providers.get(normalized);
        if (provider == null) {
            throw new IllegalStateException("No object target provider registered for: " + providerName);
        }
        return provider;
    }

    public Map<String, ObjectTargetProvider> providers() {
        return providers;
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("object target provider name must not be blank");
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }
}
