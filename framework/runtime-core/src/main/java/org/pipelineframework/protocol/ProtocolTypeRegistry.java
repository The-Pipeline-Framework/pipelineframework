package org.pipelineframework.protocol;

import java.util.*;

import org.pipelineframework.connector.ConnectorProviderManifestCatalog;

/** Deterministically validated build-time catalog of contributed protocol vocabulary. */
public final class ProtocolTypeRegistry {
    private final Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> descriptors;

    public ProtocolTypeRegistry(
        Collection<ProtocolTypeDescriptor> frameworkTypes,
        ConnectorProviderManifestCatalog connectorCatalog
    ) {
        Objects.requireNonNull(frameworkTypes, "framework protocol types must not be null");
        Objects.requireNonNull(connectorCatalog, "connector manifest catalog must not be null");
        List<ProtocolTypeDescriptor> ordered = new ArrayList<>(frameworkTypes);
        ordered.addAll(connectorCatalog.protocolTypes().values());
        ordered.sort(Comparator.comparing(ProtocolTypeDescriptor::identity));
        Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> indexed = new LinkedHashMap<>();
        for (ProtocolTypeDescriptor descriptor : ordered) {
            ProtocolTypeDescriptor checked = Objects.requireNonNull(descriptor, "protocol type descriptor must not be null");
            if (indexed.putIfAbsent(checked.identity(), checked) != null) {
                throw new IllegalArgumentException("duplicate protocol type identity: " + checked.identity());
            }
        }
        descriptors = Collections.unmodifiableMap(indexed);
    }

    public static ProtocolTypeRegistry empty() {
        return new ProtocolTypeRegistry(List.of(), new ConnectorProviderManifestCatalog(List.of()));
    }

    /** Discover extension-owned protocol vocabulary without constructing connector providers. */
    public static List<ProtocolTypeDescriptor> discoverContributions(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "protocol type contribution class loader must not be null");
        List<ProtocolTypeDescriptor> result = new ArrayList<>();
        ServiceLoader.load(ProtocolTypeContributor.class, classLoader).stream()
            .sorted(Comparator.comparing(provider -> provider.type().getName()))
            .map(ServiceLoader.Provider::get)
            .forEach(contributor -> result.addAll(Objects.requireNonNull(
                contributor.protocolTypes(), "protocol type contributions must not be null")));
        return List.copyOf(result);
    }

    public Map<ProtocolTypeIdentity, ProtocolTypeDescriptor> descriptors() {
        return descriptors;
    }

    public ProtocolTypeDescriptor resolve(String reference) {
        Objects.requireNonNull(reference, "protocol type reference must not be null");
        String token = reference.trim();
        if (token.contains(".")) {
            ProtocolTypeIdentity identity = ProtocolTypeIdentity.of(token);
            ProtocolTypeDescriptor descriptor = descriptors.get(identity);
            if (descriptor == null) {
                throw new IllegalArgumentException("unknown contributed protocol type '<" + token + ">'" );
            }
            return descriptor;
        }
        List<ProtocolTypeDescriptor> matches = descriptors.values().stream()
            .filter(descriptor -> descriptor.identity().typeName().equals(token))
            .toList();
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("unknown contributed protocol type '<" + token + ">'" );
        }
        if (matches.size() > 1) {
            String candidates = matches.stream().map(descriptor -> descriptor.identity().qualifiedName())
                .sorted().collect(java.util.stream.Collectors.joining(", "));
            throw new IllegalArgumentException("ambiguous contributed protocol type '<" + token
                + ">'; use one of: " + candidates);
        }
        return matches.getFirst();
    }
}
