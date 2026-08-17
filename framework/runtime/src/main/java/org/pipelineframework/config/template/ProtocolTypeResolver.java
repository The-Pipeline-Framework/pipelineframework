package org.pipelineframework.config.template;

import java.util.*;

import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;
import org.pipelineframework.protocol.ProtocolTypeRegistry;

/** Resolves contributed references once, before ordinary v3 validation and generation. */
final class ProtocolTypeResolver {
    private final ProtocolTypeRegistry registry;
    private final Map<String, PipelineTemplateTypeDefinition> definitions;
    private final Map<String, ProtocolTypeIdentity> identities = new LinkedHashMap<>();
    private final Set<ProtocolTypeIdentity> importing = new LinkedHashSet<>();

    ProtocolTypeResolver(ProtocolTypeRegistry registry, Map<String, PipelineTemplateTypeDefinition> authoredDefinitions) {
        this.registry = Objects.requireNonNull(registry, "protocol type registry must not be null");
        this.definitions = new LinkedHashMap<>(Objects.requireNonNull(authoredDefinitions, "authored types must not be null"));
    }

    Resolved resolve(
        Map<String, Map<String, RepresentationMapping>> representationMappings,
        Map<String, Map<String, Object>> representationProviderConfigurations,
        String inputContract,
        String outputContract,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateDefinition> pipelines
    ) {
        List<String> authoredNames = List.copyOf(definitions.keySet());
        for (String name : authoredNames) {
            definitions.put(name, normalizeDefinition(definitions.get(name)));
        }
        String resolvedInput = normalizeNullableContract(inputContract);
        String resolvedOutput = normalizeNullableContract(outputContract);
        List<PipelineTemplateStep> resolvedSteps = steps.stream().map(this::normalizeStep).toList();
        Map<String, PipelineTemplateDefinition> resolvedPipelines = new LinkedHashMap<>();
        pipelines.forEach((id, pipeline) -> resolvedPipelines.put(id, new PipelineTemplateDefinition(
            normalizeNullableContract(pipeline.inputContract()),
            normalizeNullableContract(pipeline.outputContract()),
            pipeline.steps().stream().map(this::normalizeStep).toList())));
        PipelineTemplateTypeModel model = new PipelineTemplateTypeModel(
            definitions, representationMappings, representationProviderConfigurations, identities);
        return new Resolved(model, resolvedInput, resolvedOutput, resolvedSteps, Map.copyOf(resolvedPipelines));
    }

    private PipelineTemplateStep normalizeStep(PipelineTemplateStep step) {
        return new PipelineTemplateStep(
            step.name(), step.cardinality(), normalizeNullableContract(step.inputTypeName()), step.inputFields(),
            step.inboundMapper(), normalizeNullableContract(step.outputTypeName()), step.outputFields(), step.outboundMapper(),
            step.execution(), step.accepts().stream().map(this::normalizeNullableContract).toList(), step.terminal(),
            step.pipelineReference(), step.callables().entrySet().stream().collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey,
                entry -> new org.pipelineframework.config.pipeline.PipelineYamlCallable(
                    entry.getValue().alias(), entry.getValue().using(), entry.getValue().operation(),
                    entry.getValue().kind(), entry.getValue().operationVersion(),
                    normalizeNullableContract(entry.getValue().input())),
                (left, right) -> { throw new IllegalStateException("duplicate callable alias: " + left.alias()); },
                LinkedHashMap::new)));
    }

    private String normalizeNullableContract(String contract) {
        if (contract == null) {
            return null;
        }
        return normalizeContract(contract);
    }

    private String normalizeContract(String contract) {
        String token = contract.trim();
        final Optional<String> contributed;
        try {
            contributed = ProtocolTypeReferences.parseContributed(token);
        } catch (IllegalArgumentException failure) {
            throw new IllegalStateException("Unsupported contributed protocol type reference '" + contract + "'", failure);
        }
        if (contributed.isEmpty()) {
            return token;
        }
        return importType(registry.resolve(contributed.orElseThrow())).identity().typeName();
    }

    private PipelineTemplateTypeDefinition normalizeDefinition(PipelineTemplateTypeDefinition definition) {
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            return new PipelineTemplateTypeDefinition.RecordType(record.name(), record.fields().stream()
                .map(field -> new PipelineTemplateTypeDefinition.Field(field.name(), normalizeReference(field.type())))
                .toList());
        }
        if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
            return new PipelineTemplateTypeDefinition.AliasType(alias.name(), normalizeReference(alias.target()));
        }
        if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
            Map<String, PipelineTemplateTypeDefinition.Variant> variants = new LinkedHashMap<>();
            union.variants().forEach((key, variant) -> variants.put(key,
                new PipelineTemplateTypeDefinition.Variant(variant.discriminator(), normalizeReference(variant.payload()))));
            return new PipelineTemplateTypeDefinition.UnionType(union.name(), variants);
        }
        return definition;
    }

    private PipelineTemplateTypeReference normalizeReference(PipelineTemplateTypeReference reference) {
        if (reference instanceof PipelineTemplateTypeReference.Contributed contributed) {
            ProtocolTypeDescriptor descriptor = importType(registry.resolve(contributed.name()));
            return new PipelineTemplateTypeReference.Named(descriptor.identity().typeName());
        }
        if (reference instanceof PipelineTemplateTypeReference.MapType map) {
            return new PipelineTemplateTypeReference.MapType(map.keyType(), normalizeReference(map.valueType()));
        }
        return reference;
    }

    private ProtocolTypeDescriptor importType(ProtocolTypeDescriptor descriptor) {
        ProtocolTypeIdentity identity = descriptor.identity();
        ProtocolTypeIdentity existingIdentity = identities.get(identity.typeName());
        if (identity.equals(existingIdentity)) {
            return descriptor;
        }
        if (definitions.containsKey(identity.typeName())) {
            String owner = existingIdentity == null ? "application-authored type" : "contributed type '" + existingIdentity + "'";
            throw new IllegalStateException("Contributed protocol type '" + identity + "' conflicts with " + owner
                + " named '" + identity.typeName() + "'");
        }
        if (!importing.add(identity)) {
            List<String> cycle = new ArrayList<>(importing.stream().map(ProtocolTypeIdentity::qualifiedName).toList());
            cycle.add(identity.qualifiedName());
            throw new IllegalStateException("Recursive contributed protocol type reference is not supported: "
                + String.join(" -> ", cycle));
        }
        PipelineTemplateTypeDefinition normalized = normalizeDefinition(descriptor.definition());
        importing.remove(identity);
        definitions.put(identity.typeName(), normalized);
        identities.put(identity.typeName(), identity);
        return descriptor;
    }

    record Resolved(
        PipelineTemplateTypeModel typeModel,
        String inputContract,
        String outputContract,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateDefinition> pipelines
    ) {
    }
}
