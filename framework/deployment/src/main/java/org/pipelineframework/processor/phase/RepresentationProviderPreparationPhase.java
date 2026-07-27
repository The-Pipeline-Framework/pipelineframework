package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.representation.RepresentationProviderRegistry;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderConfiguration;
import org.pipelineframework.representation.spi.ProviderDiagnostic;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;
import org.pipelineframework.representation.spi.ResolvedRepresentation;

/**
 * Temporary annotation-processor host bridge for the host-neutral provider lifecycle. It resolves provider ownership
 * before the core classifies an internal service and leaves ordinary steps untouched when no provider claims them.
 */
public final class RepresentationProviderPreparationPhase implements PipelineCompilationPhase {
    public RepresentationProviderPreparationPhase() {
    }

    @Override
    public String name() {
        return "Representation Provider Preparation Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config) || config.version() != 3) {
            return;
        }
        RepresentationProviderRegistry providers = RepresentationProviderRegistry.discover(
            ctx.getRepresentationProviderClassLoader());
        ctx.setRepresentationProviderRegistry(providers);
        reportDiagnostics(ctx, providers.validate(globalConfigurations(config)));
        for (StepDefinition step : ctx.getStepDefinitions()) {
            resolveBoundary(ctx, config, providers, step);
        }
    }

    private void resolveBoundary(PipelineCompilationContext ctx, PipelineTemplateConfig config,
                                 RepresentationProviderRegistry providers, StepDefinition step) {
        if (step.executionClass() == null || step.inputType() == null || step.outputType() == null) {
            return;
        }
        CanonicalType input = canonical(config, step.inputType());
        CanonicalType output = canonical(config, step.outputType());
        BoundaryRequest request = new BoundaryRequest(step.name(), step.executionClass().canonicalName(), input, output,
            step.streamingShapeHint() == null ? "UNARY_UNARY" : step.streamingShapeHint().name(),
            boundaryContracts(ctx, step.executionClass().canonicalName()), Map.of());
        Optional<BoundaryClaim> claim = providers.resolveClaim(request);
        if (claim.isEmpty()) {
            return;
        }
        RepresentationMapping mapping = config.typeModel().representationMapping(output.name(), claim.get().providerKey())
            .orElseThrow(() -> new IllegalStateException("Representation provider '" + claim.get().providerKey()
                + "' claimed boundary '" + step.name() + "' but canonical type '" + output.name()
                + "' has no matching mapping (type/key)."));
        ResolvedRepresentation resolved = providers.resolve(new RepresentationMappingRequest(mapping.key(), output,
            mapping.representationType(), mapping.mapperType(), mapping.options()))
            .orElseThrow(() -> new IllegalStateException("Representation provider '" + claim.get().providerKey()
                + "' did not resolve mapping for type/key '" + output.name() + "/" + mapping.key() + "'."));
        validateClasses(ctx, resolved);
        ctx.getResolvedRepresentationRegistry().register(resolved);
        ctx.registerResolvedProviderBoundary(new ResolvedProviderBoundary(request, claim.get(), List.of(resolved),
            config.typeModel().representationProviderConfiguration(claim.get().providerKey()).orElse(Map.of())));
    }

    private static List<ProviderConfiguration> globalConfigurations(PipelineTemplateConfig config) {
        return config.typeModel().representationProviderConfigurations().entrySet().stream()
            .map(entry -> new ProviderConfiguration(org.pipelineframework.representation.spi.RepresentationScope.GLOBAL,
                entry.getKey(), entry.getValue()))
            .toList();
    }

    private static CanonicalType canonical(PipelineTemplateConfig config, ClassName javaType) {
        String name = javaType.simpleName();
        CanonicalTypeShape shape = config.typeModel().definition(name)
            .map(RepresentationProviderPreparationPhase::shape)
            .orElse(CanonicalTypeShape.UNKNOWN);
        return new CanonicalType(name, javaType.canonicalName(), shape);
    }

    private static CanonicalTypeShape shape(PipelineTemplateTypeDefinition definition) {
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType) {
            return CanonicalTypeShape.RECORD;
        }
        if (definition instanceof PipelineTemplateTypeDefinition.WrapperType) {
            return CanonicalTypeShape.WRAPPER;
        }
        if (definition instanceof PipelineTemplateTypeDefinition.AliasType) {
            return CanonicalTypeShape.ALIAS;
        }
        if (definition instanceof PipelineTemplateTypeDefinition.UnionType) {
            return CanonicalTypeShape.UNION;
        }
        return CanonicalTypeShape.UNKNOWN;
    }

    private static Set<String> boundaryContracts(PipelineCompilationContext ctx, String serviceTypeName) {
        TypeElement type = ctx.getProcessingEnv().getElementUtils().getTypeElement(serviceTypeName);
        if (type == null) {
            return Set.of();
        }
        Set<String> contracts = new LinkedHashSet<>();
        collectContracts(ctx, type.asType(), contracts);
        return Set.copyOf(contracts);
    }

    private static void collectContracts(PipelineCompilationContext ctx, TypeMirror type, Set<String> contracts) {
        for (TypeMirror supertype : ctx.getProcessingEnv().getTypeUtils().directSupertypes(type)) {
            TypeElement element = (TypeElement) ctx.getProcessingEnv().getTypeUtils().asElement(supertype);
            if (element != null && contracts.add(element.getQualifiedName().toString())) {
                collectContracts(ctx, supertype, contracts);
            }
        }
    }

    private static void validateClasses(PipelineCompilationContext ctx, ResolvedRepresentation representation) {
        representation.representationType().ifPresent(type -> requireType(ctx, type, representation, "representation type"));
        representation.mapperType().ifPresent(type -> requireType(ctx, type, representation, "mapper type"));
        if (representation.representationType().isPresent() && representation.mapperType().isPresent()) {
            validateMapperPair(ctx, representation);
        }
    }

    private static void validateMapperPair(PipelineCompilationContext ctx, ResolvedRepresentation representation) {
        TypeElement mapper = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(representation.mapperType().orElseThrow());
        TypeElement domain = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(representation.domainType().targetTypeName());
        TypeElement external = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(representation.representationType().orElseThrow());
        if (mapper == null || domain == null || external == null) {
            return;
        }
        var mapperPair = findMapperSupertype(ctx, mapper.asType());
        if (mapperPair.isEmpty() || mapperPair.orElseThrow().getTypeArguments().size() != 2
                || !ctx.getProcessingEnv().getTypeUtils().isSameType(mapperPair.orElseThrow().getTypeArguments().getFirst(), domain.asType())
                || !ctx.getProcessingEnv().getTypeUtils().isSameType(mapperPair.orElseThrow().getTypeArguments().get(1), external.asType())) {
            throw new IllegalStateException("Representation provider '" + representation.providerKey() + "' requires mapper '"
                + representation.mapperType().orElseThrow() + "' to implement exact Mapper<"
                + representation.domainType().targetTypeName() + ", "
                + representation.representationType().orElseThrow() + "> for canonical type/key '"
                + representation.domainType().name() + "/" + representation.providerKey() + "'.");
        }
    }

    private static Optional<DeclaredType> findMapperSupertype(PipelineCompilationContext ctx, TypeMirror type) {
        var element = ctx.getProcessingEnv().getTypeUtils().asElement(type);
        if (element instanceof TypeElement typeElement
                && typeElement.getQualifiedName().contentEquals("org.pipelineframework.mapper.Mapper")
                && type instanceof DeclaredType declared) {
            return Optional.of(declared);
        }
        for (TypeMirror supertype : ctx.getProcessingEnv().getTypeUtils().directSupertypes(type)) {
            Optional<DeclaredType> found = findMapperSupertype(ctx, supertype);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private static void requireType(PipelineCompilationContext ctx, String typeName, ResolvedRepresentation representation,
                                    String role) {
        if (ctx.getProcessingEnv().getElementUtils().getTypeElement(typeName) == null) {
            throw new IllegalStateException("Representation provider '" + representation.providerKey() + "' cannot resolve "
                + role + " '" + typeName + "' for canonical type/key '" + representation.domainType().name()
                + "/" + representation.providerKey() + "'.");
        }
    }

    private static void reportDiagnostics(PipelineCompilationContext ctx, List<ProviderDiagnostic> diagnostics) {
        diagnostics.forEach(diagnostic -> ctx.getProcessingEnv().getMessager().printMessage(
            diagnostic.severity() == ProviderDiagnostic.Severity.ERROR ? Diagnostic.Kind.ERROR : Diagnostic.Kind.WARNING,
            "[representation-provider:" + diagnostic.code() + "] " + diagnostic.message()));
    }
}
