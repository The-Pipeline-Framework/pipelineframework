package org.pipelineframework.protocol;

import java.util.Objects;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;

/** Immutable contribution of one canonical v3 semantic type definition. */
public record ProtocolTypeDescriptor(
    ProtocolTypeIdentity identity,
    PipelineTemplateTypeDefinition definition
) {
    public ProtocolTypeDescriptor {
        identity = Objects.requireNonNull(identity, "protocol type identity must not be null");
        definition = Objects.requireNonNull(definition, "protocol type definition must not be null");
        if (!identity.typeName().equals(definition.name())) {
            throw new IllegalArgumentException("protocol type identity '" + identity
                + "' does not match definition name '" + definition.name() + "'");
        }
        validateClosedDefinition(definition);
    }

    private static void validateClosedDefinition(PipelineTemplateTypeDefinition definition) {
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            if (record.fields().stream().map(PipelineTemplateTypeDefinition.Field::name).distinct().count()
                != record.fields().size()) {
                throw new IllegalArgumentException("protocol type '" + definition.name() + "' declares duplicate field names");
            }
            record.fields().forEach(field -> validateReference(definition.name() + "." + field.name(), field.type(), false));
        } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
            validateReference(definition.name() + ".alias", alias.target(), false);
        } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
            if (union.variants().isEmpty()) {
                throw new IllegalArgumentException("protocol union '" + definition.name() + "' must declare a variant");
            }
            union.variants().values().forEach(variant ->
                validateReference(definition.name() + "." + variant.discriminator(), variant.payload(), true));
        }
    }

    private static void validateReference(String owner, PipelineTemplateTypeReference reference, boolean namedOnly) {
        if (reference instanceof PipelineTemplateTypeReference.Contributed) {
            return;
        }
        if (!namedOnly && reference instanceof PipelineTemplateTypeReference.Scalar) {
            return;
        }
        throw new IllegalArgumentException("protocol type '" + owner
            + "' must be closed over v3 scalars and qualified contributed references");
    }
}
