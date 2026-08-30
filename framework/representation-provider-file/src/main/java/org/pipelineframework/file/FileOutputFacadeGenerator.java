package org.pipelineframework.file;

import java.util.Optional;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;

final class FileOutputFacadeGenerator {
    private FileOutputFacadeGenerator() { }

    static String generate(ProviderGenerationRequest request, FileMappingOptions input, FileMappingOptions output) {
        String inputField = input.payloadField();
        output.payloadField();
        String target = output.requiredText("target");
        String optionalKey = output.optionalText("key")
            .map(value -> "java.util.Optional.of(\"" + javaString(value) + "\")")
            .orElse("java.util.Optional.empty()");
        String canonicalInput = request.boundary().inputType().targetTypeName();
        String canonicalOutput = request.boundary().outputType().targetTypeName();
        String facade = request.claim().generatedFacadeTypeName();
        int separator = facade.lastIndexOf('.');
        String cardinality = request.boundary().cardinality();
        String service = "UNARY_STREAMING".equals(cardinality) ? "ReactiveStreamingService" : "ReactiveService";
        String reactive = "UNARY_STREAMING".equals(cardinality) ? "Multi" : "Uni";
        String operation = "UNARY_STREAMING".equals(cardinality) ? "oneToMany" : "oneToOne";
        return """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @io.quarkus.arc.Unremovable
            @org.pipelineframework.annotation.PipelineStep
            public final class %s implements org.pipelineframework.service.%s<%s, %s> {
                @jakarta.inject.Inject %s delegate;
                @jakarta.inject.Inject org.pipelineframework.file.FileRepresentationRuntime files;

                @Override
                public io.smallrye.mutiny.%s<%s> process(%s input) {
                    return files.%s(input.%s(), %dL, "%s", %dL, %s, delegate::process)
                        .map(reference -> new %s(reference));
                }
            }
            """.formatted(facade.substring(0, separator), facade.substring(separator + 1), service,
                canonicalInput, canonicalOutput, request.boundary().serviceTypeName(), reactive, canonicalOutput,
                canonicalInput, operation, inputField, input.maxBytes(), javaString(target), output.maxBytes(),
                optionalKey, canonicalOutput);
    }

    static String javaString(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                default -> {
                    if (character < 0x20) {
                        escaped.append("\\%03o".formatted((int) character));
                    } else {
                        escaped.append(character);
                    }
                }
            }
        }
        return escaped.toString();
    }
}
