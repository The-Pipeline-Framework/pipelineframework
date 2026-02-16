package org.pipelineframework.processor.ir;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/**
 * Represents a semantic directional type mapping derived from annotations. Contains semantic information from the @PipelineStep
 * annotation, including domain types and inferred mapper information.
 *
 * @param domainType the domain type for this mapping
 * @param mapperType the inferred mapper type for this mapping, or null if not yet resolved
 * @param hasMapper whether a mapper has been inferred for this mapping
 * @param entityType the entity type used for mapper inference (the domain type that the mapper operates on)
 */
public record TypeMapping(
        TypeName domainType,
        TypeName mapperType,
        boolean hasMapper,
        TypeName entityType
) {
    /**
     * Creates a new TypeMapping instance with entity type for inference.
     */
    public TypeMapping {
        // entityType defaults to domainType if not specified
        if (entityType == null) {
            entityType = domainType;
        }
    }

    /**
     * Creates a new TypeMapping instance (backward-compatible constructor).
     *
     * @param domainType the domain type
     * @param mapperType the mapper type (may be null)
     * @param hasMapper whether a mapper is present
     */
    public TypeMapping(TypeName domainType, TypeName mapperType, boolean hasMapper) {
        this(domainType, mapperType, hasMapper, domainType);
    }

    /**
     * Creates a new TypeMapping with the inferred mapper type.
     *
     * @param inferredMapperType the inferred mapper ClassName
     * @return a new TypeMapping instance with the mapper type set
     */
    public TypeMapping withInferredMapper(ClassName inferredMapperType) {
        return new TypeMapping(domainType, inferredMapperType, true, entityType);
    }
}
