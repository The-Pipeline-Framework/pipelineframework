package org.pipelineframework.opencsv;

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
import org.pipelineframework.representation.spi.ProviderGenerationRequest;
import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderSchemaFragment;
import org.pipelineframework.representation.spi.ProviderStepContract;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/** OpenCSV owns its marker, mapping requirements, facade content, and diagnostics without leaking into core generation. */
public final class OpenCsvRepresentationProvider implements RepresentationProvider {
    public static final String KEY = "opencsv";
    private static final String BOUNDARY = OpenCsvInputBoundary.class.getName();

    @Override
    public ProviderMetadata metadata() {
        return new ProviderMetadata(KEY, Set.of(), Set.of("input", "blocking-iterator", "explicit-mapper"));
    }

    @Override
    public List<ProviderDiagnostic> validate(ProviderConfiguration configuration) {
        if (!KEY.equals(configuration.providerKey())) {
            return List.of();
        }
        return List.of();
    }

    @Override
    public Optional<ResolvedRepresentation> resolve(RepresentationMappingRequest mapping) {
        if (!KEY.equals(mapping.key())) {
            return Optional.empty();
        }
        if (mapping.domainType().shape() != CanonicalTypeShape.RECORD) {
            throw new IllegalStateException("OpenCSV representation mapping for canonical type '"
                + mapping.domainType().name() + "' supports records only (key=opencsv).");
        }
        if (mapping.representationType().isEmpty() || mapping.mapperType().isEmpty()) {
            throw new IllegalStateException("OpenCSV representation mapping for canonical type '"
                + mapping.domainType().name() + "' requires both type and mapper (key=opencsv).");
        }
        return Optional.of(new ResolvedRepresentation(KEY, mapping.domainType(), mapping.representationType(), mapping.mapperType()));
    }

    @Override
    public Optional<BoundaryClaim> claim(BoundaryRequest boundary) {
        if (!boundary.declaredBoundaryContracts().contains(BOUNDARY)) {
            return Optional.empty();
        }
        String facade = boundary.serviceTypeName() + "PipelineFacade";
        return Optional.of(new BoundaryClaim(KEY, boundary.stepName() + ":opencsv", facade,
            Optional.of(new ProviderStepContract(ProviderExecutionStyle.BLOCKING_ITERATOR, "UNARY_STREAMING"))));
    }

    @Override
    public List<ArtifactDescription> describeArtifacts(ProviderGenerationRequest request) {
        ResolvedRepresentation mapping = request.representations().stream()
            .filter(candidate -> KEY.equals(candidate.providerKey())
                && candidate.domainType().name().equals(request.boundary().outputType().name()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("OpenCSV boundary '" + request.boundary().stepName()
                + "' requires an opencsv mapping for canonical type '" + request.boundary().outputType().name() + "'."));
        String source = facadeSource(request, mapping);
        String logicalPath = request.claim().generatedFacadeTypeName().replace('.', '/') + ".java";
        return List.of(new ArtifactDescription(KEY, ArtifactPhase.PRE_MODEL, ArtifactKind.JAVA_SOURCE,
            logicalPath, source, 0));
    }

    @Override
    public ProviderSchemaFragment schema() {
        return new ProviderSchemaFragment(KEY,
            Optional.of("{\"type\":\"object\",\"additionalProperties\":true}"),
            Optional.of("{\"type\":\"object\",\"properties\":{\"type\":{\"type\":\"string\"},\"mapper\":{\"type\":\"string\"},\"options\":{\"type\":\"object\"}}}"),
            Optional.of("OpenCSV input boundaries require an explicit row type and Mapper<Canonical, Row>."));
    }

    private static String facadeSource(ProviderGenerationRequest request, ResolvedRepresentation mapping) {
        String canonicalInput = request.boundary().inputType().targetTypeName();
        String canonicalOutput = request.boundary().outputType().targetTypeName();
        String external = mapping.representationType().orElseThrow();
        String mapper = mapping.mapperType().orElseThrow();
        String facade = request.claim().generatedFacadeTypeName();
        int separator = facade.lastIndexOf('.');
        String packageName = facade.substring(0, separator);
        String simpleName = facade.substring(separator + 1);
        return """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @org.pipelineframework.annotation.PipelineStep
            public final class %s implements org.pipelineframework.service.blocking.BlockingIteratorService<%s, %s> {
                @jakarta.inject.Inject
                %s delegate;

                @jakarta.inject.Inject
                %s mapper;

                @Override
                public org.pipelineframework.blocking.CloseableIterator<%s> iterateBlocking(%s input) {
                    return new MappingIterator(delegate.iterateBlocking(input), mapper);
                }

                private static final class MappingIterator implements org.pipelineframework.blocking.CloseableIterator<%s> {
                    private final org.pipelineframework.blocking.CloseableIterator<%s> delegate;
                    private final org.pipelineframework.mapper.Mapper<%s, %s> mapper;

                    private MappingIterator(org.pipelineframework.blocking.CloseableIterator<%s> delegate,
                                            org.pipelineframework.mapper.Mapper<%s, %s> mapper) {
                        this.delegate = delegate;
                        this.mapper = mapper;
                    }

                    @Override public boolean hasNext() { return delegate.hasNext(); }
                    @Override public %s next() { return mapper.fromExternal(delegate.next()); }
                    @Override public void close() throws Exception { delegate.close(); }
                }
            }
            """.formatted(packageName, simpleName, canonicalInput, canonicalOutput,
                request.boundary().serviceTypeName(), mapper, canonicalOutput, canonicalInput,
                canonicalOutput, external, canonicalOutput, external, external, canonicalOutput, external,
                canonicalOutput);
    }
}
