package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.representation.CanonicalSchemaJson;
import org.pipelineframework.processor.representation.ProviderArtifactWriter;
import org.pipelineframework.processor.representation.RepresentationProviderRegistry;
import org.pipelineframework.processor.routing.V3JavaTypeResolver;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.OperationBoundaryClaim;
import org.pipelineframework.representation.spi.OperationBoundaryRequest;
import org.pipelineframework.representation.spi.OperationProviderGenerationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRole;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.ResolvedOperationRepresentation;

/** Resolves and materializes canonical/wire representations after operation-first model extraction. */
public final class OperationRepresentationGenerationPhase implements PipelineCompilationPhase {
    private final ProviderArtifactWriter artifactWriter;

    public OperationRepresentationGenerationPhase() {
        this(new ProviderArtifactWriter());
    }

    OperationRepresentationGenerationPhase(ProviderArtifactWriter artifactWriter) {
        this.artifactWriter = artifactWriter;
    }

    @Override
    public String name() {
        return "Operation Representation Generation Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config) || config.version() != 3
            || ctx.getRepresentationProviderRegistry() == null) {
            return;
        }
        RepresentationProviderRegistry providers = ctx.getRepresentationProviderRegistry();
        Map<String, ResolvedOperationRepresentation> resolved = new LinkedHashMap<>();
        for (PipelineStepModel model : models(ctx)) {
            model.connectorOperationSelection().ifPresent(selection -> resolveIfSupported(
                ctx, config, providers, selection, () -> canonical(config, model.inputMapping()),
                () -> canonical(config, model.outputMapping()), resolved));
            model.dynamicOperationSelection().ifPresent(dynamic -> dynamic.callables().forEach(callable ->
                resolveIfSupported(ctx, config, providers, callable.operation(),
                    () -> canonical(config, callable.inputType(), callable.inputClass()),
                    () -> canonical(config, callable.outputType(), callable.outputClass()), resolved)));
        }
        Map<String, List<ResolvedOperationRepresentation>> byProvider = resolved.values().stream()
            .collect(java.util.stream.Collectors.groupingBy(ResolvedOperationRepresentation::providerKey,
                java.util.TreeMap::new, java.util.stream.Collectors.toList()));
        List<org.pipelineframework.representation.spi.ArtifactDescription> artifacts = new ArrayList<>();
        byProvider.forEach((providerKey, values) -> artifacts.addAll(providers.provider(providerKey)
            .describeOperationArtifacts(new OperationProviderGenerationRequest(values))));
        ctx.setResolvedOperationRepresentations(resolved.values().stream()
            .sorted(Comparator.comparing(ResolvedOperationRepresentation::mappingKey))
            .toList());
        artifactWriter.write(ctx.getProcessingEnv().getFiler(), artifacts);
    }

    private static List<PipelineStepModel> models(PipelineCompilationContext ctx) {
        Map<String, PipelineStepModel> values = new LinkedHashMap<>();
        ctx.getStepModels().forEach(model -> values.put(identity(model), model));
        ctx.getLocalDefinitionStepModels().values().stream().flatMap(List::stream)
            .forEach(model -> values.put(identity(model), model));
        return values.values().stream().sorted(Comparator.comparing(OperationRepresentationGenerationPhase::identity))
            .toList();
    }

    private static String identity(PipelineStepModel model) {
        return model.definition().logicalId() + ":" + model.generatedName();
    }

    private static void resolveIfSupported(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig config,
        RepresentationProviderRegistry providers,
        ConnectorOperationSelection selection,
        java.util.function.Supplier<CanonicalType> input,
        java.util.function.Supplier<CanonicalType> output,
        Map<String, ResolvedOperationRepresentation> resolved
    ) {
        if (!providers.supportsOperationProvider(
            selection.operation().providerId().value(), selection.providerMajorVersion())) {
            return;
        }
        resolve(ctx, config, providers, selection, input.get(), output.get(), resolved);
    }

    private static void resolve(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig config,
        RepresentationProviderRegistry providers,
        ConnectorOperationSelection selection,
        CanonicalType input,
        CanonicalType output,
        Map<String, ResolvedOperationRepresentation> resolved
    ) {
        OperationBoundaryRequest boundary = new OperationBoundaryRequest(
            selection.definition().logicalId() + ":" + selection.runtimeStepId() + ":" + selection.operation(),
            selection.operation().providerId().value(), selection.providerMajorVersion(),
            selection.operation().operationId(), selection.operation().kind().value(),
            selection.operation().majorVersion(), input, output);
        Optional<OperationBoundaryClaim> claim = providers.resolveOperationClaim(boundary);
        if (claim.isEmpty()) return;
        resolveRole(ctx, config, providers, boundary, claim.orElseThrow(), OperationRepresentationRole.REQUEST,
            input, claim.orElseThrow().request(), resolved);
        claim.orElseThrow().responses().forEach(response -> resolveRole(ctx, config, providers, boundary,
            claim.orElseThrow(), OperationRepresentationRole.RESPONSE, output, response, resolved));
    }

    private static void resolveRole(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig config,
        RepresentationProviderRegistry providers,
        OperationBoundaryRequest boundary,
        OperationBoundaryClaim claim,
        OperationRepresentationRole role,
        CanonicalType canonical,
        OperationBoundaryClaim.WireBoundary wire,
        Map<String, ResolvedOperationRepresentation> resolved
    ) {
        Optional<RepresentationMappingRequest> mapping = config.typeModel()
            .representationMapping(canonical.name(), wire.mappingKey())
            .map(value -> mapping(canonical, value));
        OperationRepresentationRequest request = new OperationRepresentationRequest(boundary, claim, role, canonical,
            CanonicalSchemaJson.render(config.typeModel(), canonical.name()), wire, mapping);
        ResolvedOperationRepresentation value = providers.resolveOperation(request).orElseThrow(() ->
            new IllegalStateException("Representation provider '" + claim.providerKey()
                + "' did not resolve Connector operation mapping '" + wire.mappingKey() + "'"));
        validateResolvedTypes(ctx, value);
        ResolvedOperationRepresentation previous = resolved.putIfAbsent(value.mappingKey(), value);
        if (previous != null && !sameMapping(previous, value)) {
            throw new IllegalStateException("Connector operation mapping key '" + value.mappingKey()
                + "' resolves inconsistently across selected operations");
        }
    }

    private static RepresentationMappingRequest mapping(CanonicalType canonical, RepresentationMapping value) {
        return new RepresentationMappingRequest(value.key(), canonical, value.representationType(),
            value.mapperType(), value.options());
    }

    private static boolean sameMapping(
        ResolvedOperationRepresentation left,
        ResolvedOperationRepresentation right
    ) {
        return left.role() == right.role() && left.canonicalType().equals(right.canonicalType())
            && left.mode().equals(right.mode()) && left.representationType().equals(right.representationType())
            && left.mapperType().equals(right.mapperType())
            && left.mappingFingerprint().equals(right.mappingFingerprint());
    }

    private static CanonicalType canonical(
        PipelineTemplateConfig config,
        org.pipelineframework.processor.ir.TypeMapping mapping
    ) {
        ClassName javaType = ClassName.bestGuess(mapping.domainType().toString());
        String name = mapping.canonicalTypeName().or(() -> new V3JavaTypeResolver(config).semanticType(javaType))
            .orElseThrow(() ->
            new IllegalStateException("Connector operation boundary for Java type '" + mapping.domainType()
                + "' has no canonical v3 type identity"));
        return canonical(config, name, javaType);
    }

    private static CanonicalType canonical(PipelineTemplateConfig config, String name, ClassName javaType) {
        CanonicalTypeShape shape = config.typeModel().definition(name).map(definition -> {
            if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.RecordType) {
                return CanonicalTypeShape.RECORD;
            }
            if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.WrapperType) {
                return CanonicalTypeShape.WRAPPER;
            }
            if (definition instanceof org.pipelineframework.config.template.PipelineTemplateTypeDefinition.AliasType) {
                return CanonicalTypeShape.ALIAS;
            }
            return CanonicalTypeShape.UNION;
        }).orElse(CanonicalTypeShape.UNKNOWN);
        return new CanonicalType(name, javaType.canonicalName(), shape);
    }

    private static void validateResolvedTypes(PipelineCompilationContext ctx, ResolvedOperationRepresentation value) {
        value.representationType().ifPresent(type -> requireType(ctx, type, value, "representation type"));
        value.mapperType().ifPresent(type -> {
            if (!"GENERATED".equals(value.mode())) requireType(ctx, type, value, "mapper type");
        });
        if ("CURATED".equals(value.mode())) validateMapperPair(ctx, value);
    }

    private static void requireType(
        PipelineCompilationContext ctx,
        String type,
        ResolvedOperationRepresentation value,
        String role
    ) {
        if (ctx.getProcessingEnv().getElementUtils().getTypeElement(type) == null) {
            throw new IllegalStateException("HTTP operation mapping '" + value.mappingKey()
                + "' cannot resolve " + role + " '" + type + "'");
        }
    }

    private static void validateMapperPair(PipelineCompilationContext ctx, ResolvedOperationRepresentation value) {
        javax.lang.model.element.TypeElement mapper = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(value.mapperType().orElseThrow());
        javax.lang.model.element.TypeElement domain = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(value.canonicalType().targetTypeName());
        javax.lang.model.element.TypeElement external = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(value.representationType().orElseThrow());
        if (mapper == null || domain == null || external == null) return;
        Optional<javax.lang.model.type.DeclaredType> pair = mapperPair(ctx, mapper.asType());
        if (pair.isEmpty() || pair.orElseThrow().getTypeArguments().size() != 2
            || !ctx.getProcessingEnv().getTypeUtils().isSameType(pair.orElseThrow().getTypeArguments().getFirst(),
                domain.asType())
            || !ctx.getProcessingEnv().getTypeUtils().isSameType(pair.orElseThrow().getTypeArguments().get(1),
                external.asType())) {
            throw new IllegalStateException("HTTP operation mapping '" + value.mappingKey() + "' requires mapper '"
                + value.mapperType().orElseThrow() + "' to implement exact Mapper<"
                + value.canonicalType().targetTypeName() + ", " + value.representationType().orElseThrow() + ">");
        }
    }

    private static Optional<javax.lang.model.type.DeclaredType> mapperPair(
        PipelineCompilationContext ctx,
        javax.lang.model.type.TypeMirror type
    ) {
        var element = ctx.getProcessingEnv().getTypeUtils().asElement(type);
        if (element instanceof javax.lang.model.element.TypeElement typeElement
            && typeElement.getQualifiedName().contentEquals("org.pipelineframework.mapper.Mapper")
            && type instanceof javax.lang.model.type.DeclaredType declared) return Optional.of(declared);
        for (javax.lang.model.type.TypeMirror supertype : ctx.getProcessingEnv().getTypeUtils().directSupertypes(type)) {
            Optional<javax.lang.model.type.DeclaredType> found = mapperPair(ctx, supertype);
            if (found.isPresent()) return found;
        }
        return Optional.empty();
    }
}
