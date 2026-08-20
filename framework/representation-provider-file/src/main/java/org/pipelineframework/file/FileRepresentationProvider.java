package org.pipelineframework.file;

import java.util.List;
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
    static final String PATH = "java.nio.file.Path";

    @Override public ProviderMetadata metadata() {
        return new ProviderMetadata(KEY, Set.of(), Set.of("input", "output", "path", "one-to-many"));
    }
    @Override public List<ProviderDiagnostic> validate(ProviderConfiguration configuration) { return List.of(); }

    @Override
    public Optional<ResolvedRepresentation> resolve(RepresentationMappingRequest mapping) {
        if (!KEY.equals(mapping.key())) return Optional.empty();
        if (mapping.domainType().shape() != CanonicalTypeShape.RECORD) {
            throw new IllegalStateException("File representation mapping for canonical type '"
                + mapping.domainType().name() + "' supports records only (key=file).");
        }
        boolean structured = mapping.options().containsKey("fields");
        if ((!structured && !Optional.of(PATH).equals(mapping.representationType()))
                || mapping.representationType().isEmpty() || mapping.mapperType().isPresent()) {
            throw new IllegalStateException("File representation mapping for canonical type '" + mapping.domainType().name()
                + "' requires java.nio.file.Path, or a structured input type with options.fields, and no mapper (key=file).");
        }
        return Optional.of(new ResolvedRepresentation(KEY, mapping.domainType(), mapping.representationType(), Optional.empty()));
    }

    @Override
    public Optional<BoundaryClaim> claim(BoundaryRequest boundary) {
        boolean input = FileMappingOptions.mappingKeys(boundary, "inputMappings").contains(KEY);
        boolean output = FileMappingOptions.mappingKeys(boundary, "outputMappings").contains(KEY);
        if (!input) return Optional.empty();
        if (!output && !"UNARY_UNARY".equals(boundary.cardinality())) {
            throw new IllegalStateException("Input-only file representation boundary '" + boundary.stepName()
                + "' supports ONE_TO_ONE only.");
        }
        if (!"UNARY_UNARY".equals(boundary.cardinality()) && !"UNARY_STREAMING".equals(boundary.cardinality())) {
            throw new IllegalStateException("File representation boundary '" + boundary.stepName()
                + "' supports ONE_TO_ONE and ONE_TO_MANY only.");
        }
        return Optional.of(new BoundaryClaim(KEY, boundary.stepName() + ":file",
            boundary.serviceTypeName() + "PipelineFacade",
            Optional.of(new ProviderStepContract(ProviderExecutionStyle.REACTIVE, boundary.cardinality()))));
    }

    @Override
    public List<ArtifactDescription> describeArtifacts(ProviderGenerationRequest request) {
        ResolvedRepresentation input = requireRepresentation(request, request.boundary().inputType().name());
        FileMappingOptions inputOptions = FileMappingOptions.input(request);
        boolean outputMapped = FileMappingOptions.mappingKeys(request.boundary(), "outputMappings").contains(KEY);
        String source;
        if (outputMapped) {
            ResolvedRepresentation output = requireRepresentation(request, request.boundary().outputType().name());
            if (!Optional.of(PATH).equals(input.representationType()) || !Optional.of(PATH).equals(output.representationType())) {
                throw new IllegalStateException("File-to-file boundary '" + request.boundary().stepName()
                    + "' requires java.nio.file.Path on both sides.");
            }
            source = FileOutputFacadeGenerator.generate(request, inputOptions, FileMappingOptions.output(request));
        } else {
            source = InputOnlyFileFacadeGenerator.generate(request, input, inputOptions);
        }
        String path = request.claim().generatedFacadeTypeName().replace('.', '/') + ".java";
        return List.of(new ArtifactDescription(KEY, ArtifactPhase.PRE_MODEL, ArtifactKind.JAVA_SOURCE, path, source, 0));
    }

    @Override public ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment(KEY, Optional.empty(), Optional.of("""
            {"type":"object","properties":{"type":{"type":"string","minLength":1},"options":{"type":"object","properties":{"field":{"type":"string"},"fields":{"type":"array","minItems":1,"items":{"type":"string"}},"materializeFields":{"type":"array","minItems":1,"items":{"type":"string"}},"target":{"type":"string"},"key":{"type":"string"},"maxBytes":{"type":"integer","minimum":1}}}},"required":["type"]}
            """.trim()), Optional.of("File mappings adapt payload_ref fields to Path values; output mappings declare a publish target."));
    }

    private static ResolvedRepresentation requireRepresentation(ProviderGenerationRequest request, String typeName) {
        return request.representations().stream()
            .filter(value -> KEY.equals(value.providerKey()) && typeName.equals(value.domainType().name()))
            .findFirst().orElseThrow(() -> new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' requires a file mapping for canonical type '" + typeName + "'."));
    }
}
