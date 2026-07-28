package org.pipelineframework.processor.representation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.ProviderConfiguration;
import org.pipelineframework.representation.spi.ProviderDiagnostic;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/**
 * Host-owned provider discovery and lifecycle ordering. Maven dependencies decide visibility only; provider metadata
 * decides lifecycle order and artifact ordering is deliberately handled elsewhere.
 */
public final class RepresentationProviderRegistry {
    private final Map<String, RepresentationProvider> byKey;
    private final List<RepresentationProvider> ordered;

    private RepresentationProviderRegistry(List<RepresentationProvider> providers) {
        this.byKey = index(providers);
        this.ordered = order(byKey);
    }

    public static RepresentationProviderRegistry discover(ClassLoader classLoader) {
        ClassLoader loader = classLoader == null ? RepresentationProviderRegistry.class.getClassLoader() : classLoader;
        try {
            List<RepresentationProvider> providers = ServiceLoader.load(RepresentationProvider.class, loader)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
            return new RepresentationProviderRegistry(providers);
        } catch (java.util.ServiceConfigurationError | LinkageError failure) {
            throw new IllegalStateException("Representation provider discovery failed in the active compiler host "
                + describeLoader(loader) + ". Ensure the provider JAR, its private dependencies, and the shared "
                + "representation-provider-api artifact are on the annotation-processor path. Cause: "
                + failure.getMessage(), failure);
        }
    }

    private static String describeLoader(ClassLoader loader) {
        String service = "META-INF/services/" + RepresentationProvider.class.getName();
        try {
            List<String> resources = java.util.Collections.list(loader.getResources(service)).stream()
                .map(java.net.URL::toExternalForm)
                .sorted()
                .toList();
            return loader.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(loader))
                + " (service resources=" + resources + ")";
        } catch (java.io.IOException ignored) {
            return loader.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(loader))
                + " (service resources unreadable)";
        }
    }

    public static RepresentationProviderRegistry of(Collection<? extends RepresentationProvider> providers) {
        return new RepresentationProviderRegistry(providers == null ? List.of() : List.copyOf(providers));
    }

    public List<RepresentationProvider> providers() {
        return ordered;
    }

    public List<ProviderDiagnostic> validate(List<ProviderConfiguration> configurations) {
        if (configurations == null || configurations.isEmpty()) {
            return List.of();
        }
        List<ProviderDiagnostic> diagnostics = new ArrayList<>();
        configurations.stream()
            .sorted(Comparator.comparing(ProviderConfiguration::providerKey).thenComparing(c -> c.scope().name()))
            .forEach(configuration -> {
                RepresentationProvider provider = byKey.get(configuration.providerKey());
                if (provider == null) {
                    diagnostics.add(new ProviderDiagnostic(ProviderDiagnostic.Severity.ERROR, "provider.absent",
                        "Representation provider '" + configuration.providerKey()
                            + "' is not available to the annotation-processor host for "
                            + configuration.scope() + " configuration. Application classpath visibility does not "
                            + "register a provider; add its JAR to the annotation processor path."));
                } else {
                    diagnostics.addAll(provider.validate(configuration));
                }
            });
        return List.copyOf(diagnostics);
    }

    public Optional<ResolvedRepresentation> resolve(RepresentationMappingRequest mapping) {
        RepresentationProvider provider = byKey.get(mapping.key());
        return provider == null ? Optional.empty() : provider.resolve(mapping);
    }

    /** Collect every claimant before core classification so ambiguity is deterministic and renderer-independent. */
    public Optional<BoundaryClaim> resolveClaim(BoundaryRequest request) {
        List<BoundaryClaim> claims = ordered.stream()
            .map(provider -> provider.claim(request))
            .flatMap(Optional::stream)
            .sorted(Comparator.comparing(BoundaryClaim::providerKey))
            .toList();
        if (claims.size() > 1) {
            throw new IllegalStateException("Representation boundary '" + request.stepName()
                + "' has multiple provider claimants: "
                + claims.stream().map(BoundaryClaim::providerKey).toList());
        }
        return claims.stream().findFirst();
    }

    public RepresentationProvider provider(String key) {
        RepresentationProvider provider = byKey.get(key);
        if (provider == null) {
            throw new IllegalStateException("Representation provider '" + key + "' is not available.");
        }
        return provider;
    }

    private static Map<String, RepresentationProvider> index(List<RepresentationProvider> providers) {
        Map<String, RepresentationProvider> result = new LinkedHashMap<>();
        providers.stream()
            .sorted(Comparator.comparing(provider -> provider.metadata().key()))
            .forEach(provider -> {
                String key = provider.metadata().key();
                if (result.putIfAbsent(key, provider) != null) {
                    throw new IllegalStateException("Duplicate representation provider key '" + key + "'.");
                }
            });
        return Map.copyOf(result);
    }

    private static List<RepresentationProvider> order(Map<String, RepresentationProvider> byKey) {
        List<RepresentationProvider> result = new ArrayList<>();
        Set<String> visiting = new LinkedHashSet<>();
        Set<String> complete = new LinkedHashSet<>();
        byKey.keySet().stream().sorted().forEach(key -> visit(key, byKey, visiting, complete, result));
        return List.copyOf(result);
    }

    private static void visit(String key, Map<String, RepresentationProvider> byKey, Set<String> visiting,
                              Set<String> complete, List<RepresentationProvider> result) {
        if (complete.contains(key)) {
            return;
        }
        if (!visiting.add(key)) {
            throw new IllegalStateException("Representation provider dependency cycle: " + visiting + " -> " + key);
        }
        RepresentationProvider provider = byKey.get(key);
        provider.metadata().requiresProviders().stream().sorted().forEach(required -> {
            if (!byKey.containsKey(required)) {
                throw new IllegalStateException("Representation provider '" + key
                    + "' requires missing provider '" + required + "'.");
            }
            visit(required, byKey, visiting, complete, result);
        });
        visiting.remove(key);
        complete.add(key);
        result.add(provider);
    }
}
