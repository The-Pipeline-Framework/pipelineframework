package org.pipelineframework.file;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.pipelineframework.representation.spi.ArtifactDescription;
import org.pipelineframework.representation.spi.ArtifactKind;
import org.pipelineframework.representation.spi.ArtifactPhase;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderConfiguration;
import org.pipelineframework.representation.spi.ProviderDiagnostic;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;
import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.ProviderSchemaFragment;
import org.pipelineframework.representation.spi.ProviderStepContract;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/** Adapts canonical payload-reference records to ordinary {@code Path}-based services. */
public final class FileRepresentationProvider implements RepresentationProvider {
    public static final String KEY = "file";
    private static final String PATH = "java.nio.file.Path";
    private static final long DEFAULT_MAX_BYTES = 64L * 1024L * 1024L;

    @Override
    public ProviderMetadata metadata() {
        return new ProviderMetadata(KEY, Set.of(), Set.of("input", "output", "path", "one-to-many"));
    }

    @Override
    public List<ProviderDiagnostic> validate(ProviderConfiguration configuration) {
        return List.of();
    }

    @Override
    public Optional<ResolvedRepresentation> resolve(RepresentationMappingRequest mapping) {
        if (!KEY.equals(mapping.key())) {
            return Optional.empty();
        }
        if (mapping.domainType().shape() != CanonicalTypeShape.RECORD) {
            throw new IllegalStateException("File representation mapping for canonical type '"
                + mapping.domainType().name() + "' supports records only (key=file).");
        }
        if (!Optional.of(PATH).equals(mapping.representationType()) || mapping.mapperType().isPresent()) {
            throw new IllegalStateException("File representation mapping for canonical type '"
                + mapping.domainType().name() + "' requires type java.nio.file.Path and no mapper (key=file).");
        }
        return Optional.of(new ResolvedRepresentation(KEY, mapping.domainType(), mapping.representationType(),
            Optional.empty()));
    }

    @Override
    public Optional<BoundaryClaim> claim(BoundaryRequest boundary) {
        if (!mappingKeys(boundary, "inputMappings").contains(KEY)
                || !mappingKeys(boundary, "outputMappings").contains(KEY)) {
            return Optional.empty();
        }
        if (!"UNARY_UNARY".equals(boundary.cardinality()) && !"UNARY_STREAMING".equals(boundary.cardinality())) {
            throw new IllegalStateException("File representation boundary '" + boundary.stepName()
                + "' supports ONE_TO_ONE and ONE_TO_MANY only.");
        }
        String facade = boundary.serviceTypeName() + "PipelineFacade";
        return Optional.of(new BoundaryClaim(KEY, boundary.stepName() + ":file", facade,
            Optional.of(new ProviderStepContract(ProviderExecutionStyle.REACTIVE, boundary.cardinality()))));
    }

    @Override
    public List<ArtifactDescription> describeArtifacts(ProviderGenerationRequest request) {
        requireRepresentation(request, request.boundary().inputType().name());
        requireRepresentation(request, request.boundary().outputType().name());
        Map<String, Object> inputOptions = options(request, "input");
        Map<String, Object> outputOptions = options(request, "output");
        String inputField = payloadField(request.boundary(), "inputFields", inputOptions);
        String outputField = payloadField(request.boundary(), "outputFields", outputOptions);
        String target = requiredText(outputOptions, "target");
        long inputMaxBytes = positiveLong(inputOptions, "maxBytes", DEFAULT_MAX_BYTES);
        long outputMaxBytes = positiveLong(outputOptions, "maxBytes", DEFAULT_MAX_BYTES);
        Optional<String> key = optionalText(outputOptions, "key");
        String source = facadeSource(request, inputField, target, inputMaxBytes, outputMaxBytes, key);
        String logicalPath = request.claim().generatedFacadeTypeName().replace('.', '/') + ".java";
        return List.of(new ArtifactDescription(KEY, ArtifactPhase.PRE_MODEL, ArtifactKind.JAVA_SOURCE,
            logicalPath, source, 0));
    }

    @Override
    public ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment(KEY, Optional.empty(), Optional.of("""
            {"type":"object","properties":{"type":{"const":"java.nio.file.Path"},"options":{"type":"object","properties":{"field":{"type":"string"},"target":{"type":"string"},"key":{"type":"string"},"maxBytes":{"type":"integer","minimum":1}}}},"required":["type"]}
            """.trim()), Optional.of("File mappings adapt a single payload_ref field to Path; output mappings declare a publish target."));
    }

    private static String facadeSource(ProviderGenerationRequest request, String inputField,
                                       String target, long inputMaxBytes, long outputMaxBytes, Optional<String> key) {
        String canonicalInput = request.boundary().inputType().targetTypeName();
        String canonicalOutput = request.boundary().outputType().targetTypeName();
        String facade = request.claim().generatedFacadeTypeName();
        int separator = facade.lastIndexOf('.');
        String packageName = facade.substring(0, separator);
        String simpleName = facade.substring(separator + 1);
        String optionalKey = key.map(value -> "java.util.Optional.of(\"" + javaString(value) + "\")")
            .orElse("java.util.Optional.empty()");
        if ("UNARY_STREAMING".equals(request.boundary().cardinality())) {
            return """
                package %s;

                @jakarta.enterprise.context.ApplicationScoped
                @org.pipelineframework.annotation.PipelineStep
                public final class %s implements org.pipelineframework.service.ReactiveStreamingService<%s, %s> {
                    @jakarta.inject.Inject %s delegate;
                    @jakarta.inject.Inject org.pipelineframework.file.FileRepresentationRuntime files;

                    @Override
                    public io.smallrye.mutiny.Multi<%s> process(%s input) {
                        return files.oneToMany(input.%s(), %dL, "%s", %dL, %s, delegate::process)
                            .map(reference -> new %s(reference));
                    }
                }
                """.formatted(packageName, simpleName, canonicalInput, canonicalOutput,
                    request.boundary().serviceTypeName(), canonicalOutput, canonicalInput, inputField,
                    inputMaxBytes, javaString(target), outputMaxBytes, optionalKey, canonicalOutput);
        }
        return """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @org.pipelineframework.annotation.PipelineStep
            public final class %s implements org.pipelineframework.service.ReactiveService<%s, %s> {
                @jakarta.inject.Inject %s delegate;
                @jakarta.inject.Inject org.pipelineframework.file.FileRepresentationRuntime files;

                @Override
                public io.smallrye.mutiny.Uni<%s> process(%s input) {
                    return files.oneToOne(input.%s(), %dL, "%s", %dL, %s, delegate::process)
                        .map(reference -> new %s(reference));
                }
            }
            """.formatted(packageName, simpleName, canonicalInput, canonicalOutput,
                request.boundary().serviceTypeName(), canonicalOutput, canonicalInput, inputField,
                inputMaxBytes, javaString(target), outputMaxBytes, optionalKey, canonicalOutput);
    }

    private static void requireRepresentation(ProviderGenerationRequest request, String typeName) {
        request.representations().stream()
            .filter(candidate -> KEY.equals(candidate.providerKey()) && typeName.equals(candidate.domainType().name()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' requires a file mapping for canonical type '" + typeName + "'."));
    }

    @SuppressWarnings("unchecked")
    private static Set<String> mappingKeys(BoundaryRequest boundary, String name) {
        Object value = boundary.configuration().get(name);
        if (!(value instanceof List<?> list)) {
            return Set.of();
        }
        return list.stream().filter(String.class::isInstance).map(String.class::cast)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> options(ProviderGenerationRequest request, String role) {
        Object value = request.globalConfiguration().get(role);
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' has no " + role + " mapping options.");
        }
        return (Map<String, Object>) map;
    }

    private static String payloadField(BoundaryRequest boundary, String configurationKey, Map<String, Object> options) {
        Map<String, String> fields = stringMap(boundary.configuration().get(configurationKey));
        Optional<String> configured = optionalText(options, "field");
        String field = configured.orElseGet(() -> fields.entrySet().stream()
            .filter(entry -> "payload_ref".equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("File boundary '" + boundary.stepName()
                + "' requires a payload_ref field.")));
        if (fields.size() != 1 || !"payload_ref".equals(fields.get(field))) {
            throw new IllegalStateException("File boundary '" + boundary.stepName()
                + "' requires its mapped record to contain exactly one payload_ref field.");
        }
        return field;
    }

    private static Map<String, String> stringMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        java.util.LinkedHashMap<String, String> result = new java.util.LinkedHashMap<>();
        map.forEach((key, fieldType) -> {
            if (key instanceof String name && fieldType instanceof String type) {
                result.put(name, type);
            }
        });
        return Map.copyOf(result);
    }

    private static String requiredText(Map<String, Object> options, String name) {
        return optionalText(options, name).orElseThrow(() ->
            new IllegalStateException("File output mapping requires option '" + name + "'."));
    }

    private static Optional<String> optionalText(Map<String, Object> options, String name) {
        Object value = options.get(name);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String text) || text.isBlank()) {
            throw new IllegalStateException("File mapping option '" + name + "' must be a non-blank string.");
        }
        return Optional.of(text.trim());
    }

    private static long positiveLong(Map<String, Object> options, String name, long defaultValue) {
        Object value = options.get(name);
        long resolved;
        if (value == null) {
            resolved = defaultValue;
        } else if (value instanceof Number number) {
            if ((number instanceof Float || number instanceof Double) && number.doubleValue() % 1 != 0.0D) {
                throw new IllegalStateException("File mapping option '" + name + "' must be a positive integer.");
            }
            resolved = number.longValue();
        } else {
            try {
                resolved = Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                throw new IllegalStateException("File mapping option '" + name + "' must be a positive integer.", e);
            }
        }
        if (resolved <= 0) {
            throw new IllegalStateException("File mapping option '" + name + "' must be a positive integer.");
        }
        return resolved;
    }

    private static String javaString(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
