package org.pipelineframework.processor.block;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.pipelineframework.config.pipeline.PipelineYamlDocumentLoader;
import org.pipelineframework.connector.ConnectorConfigSchemaDescriptor;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/** Discovers block manifests and normalizes their v3 definitions into one ordinary compiler input. */
public final class BlockDefinitionImporter {
    public static final String MANIFEST_RESOURCE = "META-INF/pipeline/blocks.json";
    private static final Set<String> SUPPORTED_FRAGMENT_KEYS = Set.of("version", "types", "representations", "pipelines");
    private static final Set<String> FORBIDDEN_STEP_KEYS = Set.of(
        "operator", "delegate", "query", "command", "connector", "await", "checkpoint");
    private static final Set<String> FORBIDDEN_KINDS = Set.of(
        "await", "remote", "operator", "delegated");
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final ClassLoader classLoader;
    private final List<URL> additionalManifestResources;
    private ConnectorProviderManifestCatalog providerManifestCatalog;

    public BlockDefinitionImporter(ClassLoader classLoader) {
        this(classLoader, List.of());
    }

    public BlockDefinitionImporter(ClassLoader classLoader, List<URL> additionalManifestResources) {
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader must not be null");
        this.additionalManifestResources = additionalManifestResources == null
            ? List.of() : List.copyOf(additionalManifestResources);
    }

    public ImportedPipelineSources importInto(Path applicationConfig) {
        Objects.requireNonNull(applicationConfig, "applicationConfig must not be null");
        Map<String, Object> application = document(applicationConfig);
        Map<String, Object> blockBindings = removeMap(application, "blockBindings");
        List<ManifestResource> manifests = discoverManifests();
        if (manifests.isEmpty()) {
            if (!blockBindings.isEmpty()) {
                throw new IllegalStateException("blockBindings references Blocks that are not installed: "
                    + String.join(", ", blockBindings.keySet()));
            }
            return new ImportedPipelineSources(applicationConfig, List.of(), false);
        }

        if (integer(application.get("version")) != 3) {
            throw new IllegalStateException("Packaged blocks require an application using version: 3.");
        }
        Map<String, Object> applicationTypes = map(application, "types", true);
        Map<String, Object> applicationPipelines = map(application, "pipelines", true);
        Map<String, Object> applicationRepresentations = map(application, "representations", true);
        Map<String, Object> applicationConnectors = map(application, "connectors", true);

        List<PackageSource> packages = manifests.stream().map(this::loadPackage).toList();
        Map<String, List<String>> importedByShortName = new LinkedHashMap<>();
        Map<String, PackageDefinition> definitionsByQualifiedId = new LinkedHashMap<>();
        for (PackageSource source : packages) {
            for (PackageDefinition definition : source.definitions()) {
                if (applicationPipelines.containsKey(definition.name())) {
                    throw new IllegalStateException("Local pipeline definition '" + definition.name()
                        + "' collides with imported block '" + definition.qualifiedId() + "'.");
                }
                if (applicationPipelines.containsKey(definition.qualifiedId())) {
                    throw new IllegalStateException("Local pipeline definition '" + definition.qualifiedId()
                        + "' collides with imported block of the same qualified identity.");
                }
                PackageDefinition previous = definitionsByQualifiedId.putIfAbsent(definition.qualifiedId(), definition);
                if (previous != null) {
                    throw new IllegalStateException("Multiple block artifacts contribute qualified definition '"
                        + definition.qualifiedId() + "'.");
                }
                importedByShortName.computeIfAbsent(definition.name(), ignored -> new ArrayList<>())
                    .add(definition.qualifiedId());
            }
        }
        validateBlockBindingTargets(blockBindings, definitionsByQualifiedId);

        mergeNamedDeclarations(applicationTypes, packages, "types");
        mergeNamedDeclarations(applicationRepresentations, packages, "representations");
        Map<String, String> unambiguousAliases = aliases(importedByShortName);
        rewriteApplicationReferences(application, applicationPipelines.keySet(), importedByShortName, unambiguousAliases);

        List<ImportedPipelineDefinition> provenance = new ArrayList<>();
        for (PackageSource source : packages) {
            Map<String, String> packageAliases = new LinkedHashMap<>(unambiguousAliases);
            source.definitions().forEach(definition -> packageAliases.put(definition.name(), definition.qualifiedId()));
            for (PackageDefinition definition : source.definitions()) {
                Map<String, Object> normalized = mutableMap(definition.definition());
                rewritePipelineReferences(normalized, Set.of(), importedByShortName, packageAliases);
                List<ImportedPipelineDefinition.ResolvedBlockRequirement> resolvedRequirements =
                    linkRequirements(definition, normalized, blockBindings, applicationConnectors);
                applicationPipelines.put(definition.qualifiedId(), normalized);
                provenance.add(new ImportedPipelineDefinition(
                    definition.qualifiedId(), definition.name(), source.manifest().namespace(),
                    source.manifest().artifact().groupId(), source.manifest().artifact().artifactId(),
                    source.manifest().artifact().version(), definition.resource(), definition.definitionFingerprint(),
                    fingerprint(normalized), resolvedRequirements));
            }
        }

        Path merged = writeMerged(application);
        provenance.sort(Comparator.comparing(ImportedPipelineDefinition::qualifiedId));
        return new ImportedPipelineSources(merged, provenance, true);
    }

    private List<ManifestResource> discoverManifests() {
        try {
            Enumeration<URL> resources = classLoader.getResources(MANIFEST_RESOURCE);
            Map<String, URL> discovered = new LinkedHashMap<>();
            Collections.list(resources).forEach(resource -> discovered.putIfAbsent(resource.toExternalForm(), resource));
            additionalManifestResources.forEach(resource -> discovered.putIfAbsent(resource.toExternalForm(), resource));
            List<URL> ordered = discovered.values().stream()
                .sorted(Comparator.comparing(URL::toExternalForm))
                .toList();
            List<ManifestResource> manifests = new ArrayList<>();
            for (URL resource : ordered) {
                try (var reader = new java.io.InputStreamReader(resource.openStream(), StandardCharsets.UTF_8)) {
                    BlockPackageManifest manifest = GSON.fromJson(reader, BlockPackageManifest.class);
                    if (manifest == null) {
                        throw new IllegalArgumentException("manifest is empty");
                    }
                    manifests.add(new ManifestResource(resource, manifest));
                } catch (RuntimeException exception) {
                    throw new IllegalArgumentException("Invalid block manifest at " + resource + ": "
                        + diagnosticMessage(exception), exception);
                }
            }
            return List.copyOf(manifests);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to discover packaged block definitions", exception);
        }
    }

    private static String diagnosticMessage(RuntimeException exception) {
        Throwable cause = exception;
        String message = exception.getClass().getSimpleName();
        while (cause != null) {
            if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
                message = cause.getMessage();
            }
            cause = cause.getCause();
        }
        return message;
    }

    private PackageSource loadPackage(ManifestResource resource) {
        BlockPackageManifest manifest = resource.manifest();
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        List<PackageDefinition> definitions = new ArrayList<>();
        Set<String> declaredNames = new LinkedHashSet<>();
        for (BlockPackageManifest.Definition declaration : manifest.definitions()) {
            if (!declaredNames.add(declaration.name())) {
                throw new IllegalStateException("Block package '" + coordinate(manifest)
                    + "' declares definition '" + declaration.name() + "' more than once.");
            }
            Map<String, Object> fragment = documents.computeIfAbsent(declaration.resource(), ignored ->
                packageDocument(resource, declaration.resource(), manifest));
            validateFragment(fragment, manifest, declaration.resource());
            Map<String, Object> pipelines = map(fragment, "pipelines", false);
            Object rawDefinition = pipelines.get(declaration.name());
            if (!(rawDefinition instanceof Map<?, ?> definition)) {
                throw new IllegalStateException("Block package '" + coordinate(manifest) + "' declares '"
                    + declaration.name() + "' but resource '" + declaration.resource() + "' does not define it.");
            }
            validateApplicationBoundFunctionalComposition(declaration.name(), definition, declaration.requires());
            String qualifiedId = manifest.namespace() + "/" + declaration.name();
            definitions.add(new PackageDefinition(declaration.name(), qualifiedId, declaration.resource(),
                mutableMap(definition), declaration.requires(), fingerprint(mutableMap(definition))));
        }
        return new PackageSource(manifest, List.copyOf(definitions), Map.copyOf(documents));
    }

    private Map<String, Object> packageDocument(
        ManifestResource manifestResource,
        String resourcePath,
        BlockPackageManifest manifest
    ) {
        URL resource = resolveFromManifest(manifestResource.resource(), resourcePath);
        if (resource == null) {
            resource = classLoader.getResource(resourcePath);
        }
        if (resource == null) {
            throw new IllegalStateException("Block package '" + coordinate(manifest)
                + "' references missing definition resource '" + resourcePath + "'.");
        }
        try {
            return document(resource);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read block definition resource '" + resourcePath + "'", exception);
        }
    }

    private static URL resolveFromManifest(URL manifest, String resourcePath) {
        String safeResourcePath = safeResourcePath(resourcePath);
        try {
            if ("jar".equals(manifest.getProtocol())) {
                String external = manifest.toExternalForm();
                int separator = external.indexOf("!/");
                return separator < 0 ? null : new URL(external.substring(0, separator + 2) + safeResourcePath);
            }
            if ("file".equals(manifest.getProtocol())) {
                Path root = Path.of(manifest.toURI());
                for (int index = 0; index < MANIFEST_RESOURCE.split("/").length; index++) {
                    root = root.getParent();
                }
                Path packageRoot = root.normalize();
                Path resolved = packageRoot.resolve(safeResourcePath).normalize();
                if (!resolved.startsWith(packageRoot)) {
                    throw new IllegalArgumentException("Block definition resource path must remain within "
                        + "the package root: '" + resourcePath + "'.");
                }
                Path realPackageRoot = packageRoot.toRealPath();
                Path realResolved = resolved.toRealPath();
                if (!realResolved.startsWith(realPackageRoot)) {
                    throw new IllegalArgumentException("Block definition resource path must remain within "
                        + "the package root after resolving symbolic links: '" + resourcePath + "'.");
                }
                return realResolved.toUri().toURL();
            }
            return null;
        } catch (Exception exception) {
            if (exception instanceof IllegalArgumentException invalidPath) {
                throw invalidPath;
            }
            throw new IllegalStateException("Unable to resolve block resource '" + resourcePath
                + "' relative to " + manifest, exception);
        }
    }

    private static String safeResourcePath(String resourcePath) {
        String canonical = resourcePath.replace('\\', '/');
        boolean windowsAbsolute = canonical.matches("^[A-Za-z]:/.*");
        boolean traversal = java.util.Arrays.stream(canonical.split("/", -1))
            .anyMatch(".."::equals);
        Path path = Path.of(canonical);
        if (canonical.startsWith("/") || windowsAbsolute || path.isAbsolute() || traversal) {
            throw new IllegalArgumentException("Block definition resource path must be package-relative "
                + "and must not contain '..': '" + resourcePath + "'.");
        }
        return path.normalize().toString().replace(java.io.File.separatorChar, '/');
    }

    private static void validateFragment(Map<String, Object> fragment, BlockPackageManifest manifest, String resource) {
        if (integer(fragment.get("version")) != 3) {
            throw new IllegalStateException("Block definition resource '" + resource + "' from '"
                + coordinate(manifest) + "' must declare version: 3.");
        }
        for (String key : fragment.keySet()) {
            if (!SUPPORTED_FRAGMENT_KEYS.contains(key)) {
                throw new IllegalStateException("Block definition resource '" + resource
                    + "' contains unsupported application/runtime property '" + key + "'.");
            }
        }
    }

    private static void validateApplicationBoundFunctionalComposition(
        String definitionName,
        Map<?, ?> definition,
        Map<String, BlockPackageManifest.Requirement> requirements
    ) {
        Object rawSteps = definition.get("steps");
        if (!(rawSteps instanceof List<?> steps) || steps.isEmpty()) {
            throw new IllegalStateException("Imported block '" + definitionName + "' requires at least one step.");
        }
        Set<String> referencedRequirements = new LinkedHashSet<>();
        for (Object rawStep : steps) {
            if (!(rawStep instanceof Map<?, ?> step)) {
                throw new IllegalStateException("Imported block '" + definitionName + "' contains an invalid step.");
            }
            for (Object rawKey : step.keySet()) {
                String key = String.valueOf(rawKey);
                if (FORBIDDEN_STEP_KEYS.contains(key)) {
                    throw new IllegalStateException("Imported block '" + definitionName
                        + "' contains forbidden functional-core step property '" + key + "'.");
                }
            }
            Object rawKind = step.get("kind");
            String kind = rawKind == null ? "" : String.valueOf(rawKind).trim().toLowerCase(Locale.ROOT);
            if (FORBIDDEN_KINDS.contains(kind)) {
                throw new IllegalStateException("Imported block '" + definitionName
                    + "' contains forbidden step kind '" + rawKind + "'.");
            }
            if (step.get("operation") instanceof Map<?, ?>) {
                throw new IllegalStateException("Imported block '" + definitionName
                    + "' contains forbidden dynamic-operation declaration.");
            }
            if (!"query".equals(kind) && !"command".equals(kind)) {
                if (step.containsKey("using") || step.containsKey("operation") || step.containsKey("operationVersion")) {
                    throw new IllegalStateException("Imported block '" + definitionName
                        + "' may use operation/using only on QUERY or COMMAND steps.");
                }
                continue;
            }
            String requirementName = requiredText(step.get("using"), "Imported block '" + definitionName
                + "' " + kind + " step requires a non-blank using capability");
            requiredText(step.get("operation"), "Imported block '" + definitionName
                + "' " + kind + " step requires a non-blank operation");
            BlockPackageManifest.Requirement requirement = requirements.get(requirementName);
            if (requirement == null) {
                throw new IllegalStateException("Imported block '" + definitionName + "' " + kind
                    + " step references undeclared requirement '" + requirementName + "'.");
            }
            if (!requirement.kind().equals(kind.toUpperCase(Locale.ROOT))) {
                throw new IllegalStateException("Imported block '" + definitionName + "' requirement '"
                    + requirementName + "' is " + requirement.kind() + " but is used by a "
                    + kind.toUpperCase(Locale.ROOT) + " step.");
            }
            if ("command".equals(kind)) {
                for (String authority : List.of("commandIdGenerator", "duplicatePolicy", "policy")) {
                    if (step.containsKey(authority)) {
                        throw new IllegalStateException("Imported block '" + definitionName
                            + "' must not declare application-owned Command field '" + authority + "'.");
                    }
                }
            }
            referencedRequirements.add(requirementName);
        }
        for (String requirement : requirements.keySet()) {
            if (!referencedRequirements.contains(requirement)) {
                throw new IllegalStateException("Imported block '" + definitionName
                    + "' declares unused requirement '" + requirement + "'.");
            }
        }
    }

    private static String requiredText(Object value, String message) {
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalStateException(message);
        }
        return string.strip();
    }

    private static void validateBlockBindingTargets(
        Map<String, Object> blockBindings,
        Map<String, PackageDefinition> definitionsByQualifiedId
    ) {
        for (String qualifiedId : blockBindings.keySet()) {
            if (!definitionsByQualifiedId.containsKey(qualifiedId)) {
                throw new IllegalStateException("blockBindings references unknown qualified Block definition '"
                    + qualifiedId + "'.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<ImportedPipelineDefinition.ResolvedBlockRequirement> linkRequirements(
        PackageDefinition definition,
        Map<String, Object> normalizedDefinition,
        Map<String, Object> blockBindings,
        Map<String, Object> applicationConnectors
    ) {
        Object rawMappings = blockBindings.get(definition.qualifiedId());
        Map<String, Object> mappings;
        if (rawMappings == null) {
            mappings = Map.of();
        } else if (rawMappings instanceof Map<?, ?> rawMap) {
            mappings = mutableMap(rawMap);
        } else {
            throw new IllegalStateException("blockBindings entry for '" + definition.qualifiedId()
                + "' must be a map.");
        }
        if (definition.requirements().isEmpty()) {
            if (!mappings.isEmpty()) {
                throw new IllegalStateException("Block '" + definition.qualifiedId()
                    + "' declares no requirements but blockBindings supplies mappings.");
            }
            return List.of();
        }
        if (rawMappings == null) {
            throw new IllegalStateException("Block '" + definition.qualifiedId()
                + "' requires application capability mappings under blockBindings.");
        }
        for (String mappedRequirement : mappings.keySet()) {
            if (!definition.requirements().containsKey(mappedRequirement)) {
                throw new IllegalStateException("blockBindings for '" + definition.qualifiedId()
                    + "' contains unknown requirement '" + mappedRequirement + "'.");
            }
        }

        Object rawSteps = normalizedDefinition.get("steps");
        List<Object> steps = (List<Object>) rawSteps;
        List<ImportedPipelineDefinition.ResolvedBlockRequirement> resolved = new ArrayList<>();
        for (Map.Entry<String, BlockPackageManifest.Requirement> requirementEntry
            : definition.requirements().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            String requirementName = requirementEntry.getKey();
            String kind = requirementEntry.getValue().kind();
            Object rawMapping = mappings.get(requirementName);
            if (!(rawMapping instanceof Map<?, ?> mappingValues)) {
                throw new IllegalStateException("blockBindings for '" + definition.qualifiedId()
                    + "' must map required capability '" + requirementName + "'.");
            }
            Map<String, Object> mapping = mutableMap(mappingValues);
            Set<String> supportedFields = "COMMAND".equals(kind)
                ? Set.of("using", "commandIdGenerator", "duplicatePolicy", "policy")
                : Set.of("using");
            mapping.keySet().stream().filter(field -> !supportedFields.contains(field)).sorted().findFirst()
                .ifPresent(field -> {
                    throw new IllegalStateException("blockBindings requirement '" + requirementName
                        + "' for '" + definition.qualifiedId() + "' has unsupported field '" + field + "'.");
                });
            String bindingName = requiredText(mapping.get("using"), "blockBindings requirement '" + requirementName
                + "' for '" + definition.qualifiedId() + "' requires non-blank using");
            ApplicationConnectorBinding connector = applicationConnector(
                definition.qualifiedId(), requirementName, bindingName, applicationConnectors);

            String commandIdGenerator = "";
            String duplicatePolicy = "";
            Map<String, Object> commandPolicy = Map.of();
            if ("COMMAND".equals(kind)) {
                commandIdGenerator = requiredText(mapping.get("commandIdGenerator"),
                    "blockBindings Command requirement '" + requirementName
                        + "' requires commandIdGenerator");
                duplicatePolicy = requiredText(mapping.get("duplicatePolicy"),
                    "blockBindings Command requirement '" + requirementName
                        + "' requires duplicatePolicy");
                if (!mapping.containsKey("policy") || !(mapping.get("policy") instanceof Map<?, ?> rawPolicy)) {
                    throw new IllegalStateException("blockBindings Command requirement '" + requirementName
                        + "' requires an explicit policy map.");
                }
                commandPolicy = mutableMap(rawPolicy);
            }

            List<ImportedPipelineDefinition.ResolvedOperation> operations = new ArrayList<>();
            for (Object rawStep : steps) {
                Map<String, Object> step = (Map<String, Object>) rawStep;
                String stepKind = String.valueOf(step.getOrDefault("kind", "")).trim().toUpperCase(Locale.ROOT);
                String stepRequirement = String.valueOf(step.get("using")).strip();
                if (!kind.equals(stepKind) || !requirementName.equals(stepRequirement)) {
                    continue;
                }
                String operation = requiredText(step.get("operation"), "Imported operation must not be blank");
                int operationVersion = step.containsKey("operationVersion")
                    ? integer(step.get("operationVersion")) : 1;
                if (operationVersion < 1) {
                    throw new IllegalStateException("Imported block '" + definition.qualifiedId()
                        + "' operationVersion must be a positive integer.");
                }
                step.put("using", bindingName);
                if ("COMMAND".equals(kind)) {
                    step.put("commandIdGenerator", commandIdGenerator);
                    step.put("duplicatePolicy", duplicatePolicy);
                    step.put("policy", deepCopy(commandPolicy));
                }
                operations.add(new ImportedPipelineDefinition.ResolvedOperation(operation, operationVersion));
            }
            List<ImportedPipelineDefinition.ResolvedOperation> distinctOperations = operations.stream()
                .distinct()
                .sorted(Comparator.comparing(ImportedPipelineDefinition.ResolvedOperation::id)
                    .thenComparingInt(ImportedPipelineDefinition.ResolvedOperation::version))
                .toList();
            if (distinctOperations.isEmpty()) {
                throw new IllegalStateException("Block '" + definition.qualifiedId()
                    + "' declares unused requirement '" + requirementName + "'.");
            }
            resolved.add(new ImportedPipelineDefinition.ResolvedBlockRequirement(
                requirementName, kind, bindingName, connector.provider(), connector.providerVersion(),
                distinctOperations, commandIdGenerator, duplicatePolicy, commandPolicy,
                connectorConfigurationDigest(connector)));
        }
        return List.copyOf(resolved);
    }

    private ApplicationConnectorBinding applicationConnector(
        String qualifiedId,
        String requirement,
        String bindingName,
        Map<String, Object> applicationConnectors
    ) {
        Object rawBinding = applicationConnectors.get(bindingName);
        if (!(rawBinding instanceof Map<?, ?> rawMap)) {
            throw new IllegalStateException("blockBindings requirement '" + requirement + "' for '"
                + qualifiedId + "' references unknown connector binding '" + bindingName + "'.");
        }
        Map<String, Object> binding = mutableMap(rawMap);
        String provider = requiredText(binding.get("provider"), "Connector binding '" + bindingName
            + "' requires provider");
        int providerVersion = integer(binding.get("version"));
        if (providerVersion < 1) {
            throw new IllegalStateException("Connector binding '" + bindingName
                + "' requires a positive provider version.");
        }
        Map<String, Object> configuration = binding.get("config") == null
            ? Map.of() : map(binding, "config", false);
        return new ApplicationConnectorBinding(bindingName, provider, providerVersion, configuration);
    }

    private String connectorConfigurationDigest(ApplicationConnectorBinding binding) {
        ConnectorProviderId providerId = ConnectorProviderId.of(binding.provider());
        ConnectorProviderArtifactDescriptor provider = providerManifestCatalog()
            .requireProvider(providerId, binding.providerVersion());
        ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(binding.configuration());
        providerManifestCatalog().validateProviderConfiguration(
            providerId, binding.providerVersion(), document,
            "connector binding '" + binding.name() + "' provider " + providerId.value());
        Optional<ConnectorConfigSchemaDescriptor> schema = provider.provider().configurationSchema();
        return schema.map(value -> ConnectorConfigurationSnapshot.from(value, document, false).digest())
            .orElse("");
    }

    private ConnectorProviderManifestCatalog providerManifestCatalog() {
        if (providerManifestCatalog == null) {
            providerManifestCatalog = ConnectorProviderManifestLoader.load(classLoader);
        }
        return providerManifestCatalog;
    }

    private static void mergeNamedDeclarations(Map<String, Object> target, List<PackageSource> packages, String key) {
        for (PackageSource source : packages) {
            Set<String> visitedResources = new LinkedHashSet<>();
            for (PackageDefinition definition : source.definitions()) {
                if (!visitedResources.add(definition.resource())) {
                    continue;
                }
                Map<String, Object> declarations = map(source.documents().get(definition.resource()), key, true);
                for (Map.Entry<String, Object> entry : declarations.entrySet()) {
                    if (target.putIfAbsent(entry.getKey(), deepCopy(entry.getValue())) != null) {
                        throw new IllegalStateException("Imported block declaration '" + entry.getKey()
                            + "' in '" + key + "' collides with another canonical declaration.");
                    }
                }
            }
        }
    }

    private static Map<String, String> aliases(Map<String, List<String>> importedByShortName) {
        Map<String, String> aliases = new LinkedHashMap<>();
        importedByShortName.forEach((name, qualified) -> {
            if (qualified.size() == 1) {
                aliases.put(name, qualified.getFirst());
            }
        });
        return Map.copyOf(aliases);
    }

    private static void rewriteApplicationReferences(Map<String, Object> application, Set<String> localDefinitions,
                                                      Map<String, List<String>> importedByShortName,
                                                      Map<String, String> aliases) {
        rewritePipelineReferences(application, localDefinitions, importedByShortName, aliases);
    }

    @SuppressWarnings("unchecked")
    private static void rewritePipelineReferences(Object node, Set<String> localDefinitions,
                                                   Map<String, List<String>> importedByShortName,
                                                   Map<String, String> aliases) {
        if (node instanceof Map<?, ?> rawMap) {
            Map<Object, Object> map = (Map<Object, Object>) rawMap;
            Object reference = map.get("pipeline");
            if (reference != null) {
                String logicalId = String.valueOf(reference).trim();
                if (!logicalId.contains("/") && !localDefinitions.contains(logicalId)) {
                    List<String> candidates = importedByShortName.getOrDefault(logicalId, List.of());
                    if (candidates.size() > 1) {
                        throw new IllegalStateException("Pipeline reference '" + logicalId
                            + "' is ambiguous; use one of: " + String.join(", ", candidates));
                    }
                    String qualified = aliases.get(logicalId);
                    if (qualified != null) {
                        map.put("pipeline", qualified);
                    }
                }
            }
            map.values().forEach(value -> rewritePipelineReferences(value, localDefinitions, importedByShortName, aliases));
        } else if (node instanceof List<?> list) {
            list.forEach(value -> rewritePipelineReferences(value, localDefinitions, importedByShortName, aliases));
        }
    }

    private static Path writeMerged(Map<String, Object> application) {
        try {
            Path path = Files.createTempFile("tpf-block-linked-", ".yaml");
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setPrettyFlow(true);
            Files.writeString(path, new Yaml(options).dump(application), StandardCharsets.UTF_8);
            return path;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to materialize linked block definitions", exception);
        }
    }

    private static Map<String, Object> document(Path path) {
        Object root = new PipelineYamlDocumentLoader().load(path);
        if (!(root instanceof Map<?, ?> map)) {
            throw new IllegalStateException("Pipeline document root must be a map: " + path);
        }
        return mutableMap(map);
    }

    private static Map<String, Object> document(URL resource) throws IOException {
        Object root;
        try (var stream = resource.openStream()) {
            root = new PipelineYamlDocumentLoader().load(stream);
        }
        if (!(root instanceof Map<?, ?> map)) {
            throw new IllegalStateException("Block definition root must be a map: " + resource);
        }
        return mutableMap(map);
    }

    private static Map<String, Object> mutableMap(Map<?, ?> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(String.valueOf(key), deepCopy(value)));
        return copy;
    }

    private static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> map) {
            return mutableMap(map);
        }
        if (value instanceof List<?> list) {
            return list.stream().map(BlockDefinitionImporter::deepCopy).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> owner, String key, boolean create) {
        Object value = owner.get(key);
        if (value == null && create) {
            Map<String, Object> created = new LinkedHashMap<>();
            owner.put(key, created);
            return created;
        }
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalStateException("Property '" + key + "' must be a map.");
        }
        return (Map<String, Object>) raw;
    }

    private static Map<String, Object> removeMap(Map<String, Object> owner, String key) {
        Object value = owner.remove(key);
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IllegalStateException("Property '" + key + "' must be a map.");
        }
        return mutableMap(raw);
    }

    private static int integer(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (RuntimeException exception) {
            return -1;
        }
    }

    private static String fingerprint(Map<String, Object> definition) {
        return "sha256:" + hex(sha256(GSON.toJson(canonical(definition)).getBytes(StandardCharsets.UTF_8)));
    }

    private static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new java.util.TreeMap<>();
            map.forEach((key, child) -> sorted.put(String.valueOf(key), canonical(child)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(BlockDefinitionImporter::canonical).toList();
        }
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static String hex(byte[] value) {
        return java.util.HexFormat.of().formatHex(value);
    }

    private static String coordinate(BlockPackageManifest manifest) {
        return manifest.artifact().groupId() + ":" + manifest.artifact().artifactId() + ":" + manifest.artifact().version();
    }

    private record ManifestResource(URL resource, BlockPackageManifest manifest) {
    }

    private record PackageSource(
        BlockPackageManifest manifest,
        List<PackageDefinition> definitions,
        Map<String, Map<String, Object>> documents
    ) {
    }

    private record PackageDefinition(
        String name,
        String qualifiedId,
        String resource,
        Map<String, Object> definition,
        Map<String, BlockPackageManifest.Requirement> requirements,
        String definitionFingerprint
    ) {
        private PackageDefinition {
            requirements = requirements == null ? Map.of() : Map.copyOf(requirements);
        }
    }

    private record ApplicationConnectorBinding(
        String name,
        String provider,
        int providerVersion,
        Map<String, Object> configuration
    ) {
        private ApplicationConnectorBinding {
            configuration = Map.copyOf(configuration);
        }
    }
}
