package org.pipelineframework.file;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/** Generates a facade for typed records with selected materialized input and published output fields. */
final class StructuredFileTransformFacadeGenerator {
    private StructuredFileTransformFacadeGenerator() { }

    static String generate(
        ProviderGenerationRequest request,
        ResolvedRepresentation inputRepresentation,
        FileMappingOptions input,
        ResolvedRepresentation outputRepresentation,
        FileMappingOptions output
    ) {
        List<String> inputFields = input.structuredFields();
        List<String> materializedFields = input.materializedFields(inputFields);
        List<String> outputFields = output.structuredFields();
        List<String> publishedFields = output.publishedFields(outputFields);
        List<String> carriedFields = output.carriedFields(outputFields);
        if (publishedFields.isEmpty()) {
            throw new IllegalStateException("Structured file output boundary '" + request.boundary().stepName()
                + "' requires at least one payload_ref field to publish.");
        }
        Set<String> materialized = Set.copyOf(materializedFields);
        Set<String> published = Set.copyOf(publishedFields);
        Set<String> carried = Set.copyOf(carriedFields);
        if (published.stream().anyMatch(carried::contains)) {
            throw new IllegalStateException("Structured file output fields cannot be both published and carried.");
        }
        String references = materializedFields.stream()
            .map(field -> "java.util.Map.entry(\"" + javaString(field) + "\", input." + field + "())")
            .collect(Collectors.joining(", "));
        String inputArguments = inputFields.stream().map(field -> materialized.contains(field)
            ? "paths.get(\"" + javaString(field) + "\")" : "input." + field + "()")
            .collect(Collectors.joining(", "));
        String outputPaths = publishedFields.stream()
            .map(field -> "java.util.Map.entry(\"" + javaString(field) + "\", result." + field + "())")
            .collect(Collectors.joining(", "));
        String canonicalArguments = outputFields.stream().map(field -> carried.contains(field)
            ? "input." + field + "()"
            : published.contains(field) ? "references.get(\"" + javaString(field) + "\")" : "result." + field + "()")
            .collect(Collectors.joining(", "));
        String canonicalInput = request.boundary().inputType().targetTypeName();
        String canonicalOutput = request.boundary().outputType().targetTypeName();
        String representedInput = inputRepresentation.representationType().orElseThrow();
        String target = output.requiredText("target");
        String facade = request.claim().generatedFacadeTypeName();
        int separator = facade.lastIndexOf('.');
        return """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @io.quarkus.arc.Unremovable
            @org.pipelineframework.annotation.PipelineStep
            public final class %s implements org.pipelineframework.service.ReactiveService<%s, %s> {
                @jakarta.inject.Inject %s delegate;
                @jakarta.inject.Inject org.pipelineframework.file.FileRepresentationRuntime files;

                @Override
                public io.smallrye.mutiny.Uni<%s> process(%s input) {
                    return files.transformStructured(
                        org.pipelineframework.file.FileRepresentationRuntime.orderedInputs(%s), %dL,
                        "%s", %dL,
                        paths -> delegate.process(new %s(%s)),
                        result -> org.pipelineframework.file.FileRepresentationRuntime.orderedOutputs(%s),
                        (result, references) -> new %s(%s));
                }
            }
            """.formatted(facade.substring(0, separator), facade.substring(separator + 1), canonicalInput,
                canonicalOutput, request.boundary().serviceTypeName(), canonicalOutput, canonicalInput,
                references, input.maxBytes(), javaString(target), output.maxBytes(), representedInput,
                inputArguments, outputPaths, canonicalOutput, canonicalArguments);
    }

    private static String javaString(String value) {
        return FileOutputFacadeGenerator.javaString(value);
    }
}
