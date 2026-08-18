package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
        Map<String, Object> boundaryConfiguration = boundaryConfiguration(config, input, output);
        BoundaryRequest request = new BoundaryRequest(step.name(), step.executionClass().canonicalName(), input, output,
            step.streamingShapeHint() == null ? "UNARY_UNARY" : step.streamingShapeHint().name(),
            boundaryContracts(ctx, step.executionClass().canonicalName()), boundaryConfiguration);
        Optional<BoundaryClaim> claim = providers.resolveClaim(request);
        if (claim.isEmpty()) {
            return;
        }
        String providerKey = claim.orElseThrow().providerKey();
        List<MappingBinding> mappings = mappings(config, input, output, providerKey);
        if (mappings.isEmpty()) {
            throw new IllegalStateException("Representation provider '" + providerKey + "' claimed boundary '"
                + step.name() + "' but neither canonical boundary type has a matching mapping (type/key).");
        }
        List<ProviderConfiguration> providerConfigurations = mappings.stream()
            .map(binding -> typeConfiguration(claim.orElseThrow(), binding.mapping()))
            .toList();
        List<ProviderDiagnostic> diagnostics = providers.validate(providerConfigurations);
        reportDiagnostics(ctx, diagnostics);
        if (diagnostics.stream().anyMatch(diagnostic -> diagnostic.severity() == ProviderDiagnostic.Severity.ERROR)) {
            throw new IllegalStateException("Representation provider '" + providerKey
                + "' configuration is invalid for claimed boundary '" + step.name() + "'.");
        }
        List<ResolvedRepresentation> resolved = mappings.stream().map(binding -> {
            RepresentationMapping mapping = binding.mapping();
            return providers.resolve(new RepresentationMappingRequest(mapping.key(), binding.type(),
                    mapping.representationType(), mapping.mapperType(), mapping.options()))
                .orElseThrow(() -> new IllegalStateException("Representation provider '" + providerKey
                    + "' did not resolve mapping for type/key '" + binding.type().name() + "/" + mapping.key() + "'."));
        }).distinct().toList();
        resolved.forEach(representation -> {
            validateClasses(ctx, representation);
            ctx.getResolvedRepresentationRegistry().register(representation);
        });
        ctx.registerResolvedProviderBoundary(new ResolvedProviderBoundary(request, claim.orElseThrow(), resolved,
            generationConfiguration(mappings)));
    }

    private static List<MappingBinding> mappings(PipelineTemplateConfig config, CanonicalType input,
                                                  CanonicalType output, String providerKey) {
        List<MappingBinding> mappings = new ArrayList<>();
        config.typeModel().representationMapping(input.name(), providerKey)
            .ifPresent(mapping -> mappings.add(new MappingBinding("input", input, mapping)));
        config.typeModel().representationMapping(output.name(), providerKey)
            .ifPresent(mapping -> mappings.add(new MappingBinding("output", output, mapping)));
        return List.copyOf(mappings);
    }

    private static Map<String, Object> generationConfiguration(List<MappingBinding> mappings) {
        if (mappings.size() == 1 && "output".equals(mappings.getFirst().role())) {
            return mappings.getFirst().mapping().options();
        }
        Map<String, Object> configuration = new LinkedHashMap<>();
        mappings.forEach(binding -> configuration.put(binding.role(), binding.mapping().options()));
        return Map.copyOf(configuration);
    }

    private static Map<String, Object> boundaryConfiguration(PipelineTemplateConfig config, CanonicalType input,
                                                              CanonicalType output) {
        Map<String, Object> configuration = new LinkedHashMap<>();
        configuration.put("inputMappings", mappingKeys(config, input.name()));
        configuration.put("outputMappings", mappingKeys(config, output.name()));
        configuration.put("inputFields", recordFields(config, input.name()));
        configuration.put("outputFields", recordFields(config, output.name()));
        return Map.copyOf(configuration);
    }

    private static List<String> mappingKeys(PipelineTemplateConfig config, String typeName) {
        return config.typeModel().representationMappings().getOrDefault(typeName, Map.of()).keySet().stream()
            .sorted()
            .toList();
    }

    private static Map<String, String> recordFields(PipelineTemplateConfig config, String typeName) {
        return config.typeModel().definition(typeName)
            .filter(PipelineTemplateTypeDefinition.RecordType.class::isInstance)
            .map(PipelineTemplateTypeDefinition.RecordType.class::cast)
            .map(record -> record.fields().stream().collect(java.util.stream.Collectors.toMap(
                PipelineTemplateTypeDefinition.Field::name,
                field -> field.type().name(),
                (left, right) -> left,
                LinkedHashMap::new)))
            .map(Map::copyOf)
            .orElse(Map.of());
    }

    private static List<ProviderConfiguration> globalConfigurations(PipelineTemplateConfig config) {
        return config.typeModel().representationProviderConfigurations().entrySet().stream()
            .map(entry -> new ProviderConfiguration(org.pipelineframework.representation.spi.RepresentationScope.GLOBAL,
                entry.getKey(), entry.getValue()))
            .toList();
    }

    static ProviderConfiguration typeConfiguration(BoundaryClaim claim, RepresentationMapping mapping) {
        return new ProviderConfiguration(org.pipelineframework.representation.spi.RepresentationScope.TYPE,
            claim.providerKey(), mapping.options());
    }

    private record MappingBinding(String role, CanonicalType type, RepresentationMapping mapping) {
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
