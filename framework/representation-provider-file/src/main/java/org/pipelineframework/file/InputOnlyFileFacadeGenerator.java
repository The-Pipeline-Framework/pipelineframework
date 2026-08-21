package org.pipelineframework.file;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

final class InputOnlyFileFacadeGenerator {
    private InputOnlyFileFacadeGenerator() { }

    static String generate(ProviderGenerationRequest request, ResolvedRepresentation representation,
                           FileMappingOptions options) {
        List<String> fields = options.structured()
            ? options.structuredFields()
            : List.of(options.payloadField());
        List<String> materializedFields = options.materializedFields(fields);
        if (Optional.of(FileRepresentationProvider.PATH).equals(representation.representationType()) && fields.size() != 1) {
            throw new IllegalStateException("Structured file input boundary '" + request.boundary().stepName()
                + "' may use java.nio.file.Path only when options.fields contains exactly one field.");
        }
        String canonicalInput = request.boundary().inputType().targetTypeName();
        String canonicalOutput = request.boundary().outputType().targetTypeName();
        String representedInput = representation.representationType().orElseThrow();
        String facade = request.claim().generatedFacadeTypeName();
        int separator = facade.lastIndexOf('.');
        Set<String> materialized = Set.copyOf(materializedFields);
        String references = materializedFields.stream()
            .map(field -> "java.util.Map.entry(\"" + javaString(field) + "\", input." + field + "())")
            .collect(Collectors.joining(", "));
        String arguments = fields.stream().map(field -> materialized.contains(field)
            ? "paths.get(\"" + javaString(field) + "\")" : "input." + field + "()")
            .collect(Collectors.joining(", "));
        String adapted = FileRepresentationProvider.PATH.equals(representedInput)
            ? arguments : "new " + representedInput + "(" + arguments + ")";
        return """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @org.pipelineframework.annotation.PipelineStep
            public final class %s implements org.pipelineframework.service.ReactiveService<%s, %s> {
                @jakarta.inject.Inject %s delegate;
                @jakarta.inject.Inject org.pipelineframework.file.FileRepresentationRuntime files;

                @Override
                public io.smallrye.mutiny.Uni<%s> process(%s input) {
                    return files.withMaterialized(
                        org.pipelineframework.file.FileRepresentationRuntime.orderedInputs(%s), %dL,
                        paths -> delegate.process(%s));
                }
            }
            """.formatted(facade.substring(0, separator), facade.substring(separator + 1), canonicalInput,
                canonicalOutput, request.boundary().serviceTypeName(), canonicalOutput, canonicalInput,
                references, options.maxBytes(), adapted);
    }

    private static String javaString(String value) { return value.replace("\\", "\\\\").replace("\"", "\\\""); }
}
