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
     * Backward-compatible constructor that creates a TypeMapping and defaults the entityType to the provided domainType.
     *
     * @param domainType the domain type for this mapping; used as the entityType when none is provided
     * @param mapperType the mapper type, or null if not specified
     * @param hasMapper  true if a mapper has been inferred, false otherwise
     */
    public TypeMapping(TypeName domainType, TypeName mapperType, boolean hasMapper) {
        this(domainType, mapperType, hasMapper, domainType);
    }

    /**
     * Checks if this mapping has an associated mapper.
     *
     * @return true if a mapper is present, false otherwise
     */
    @Override
    public boolean hasMapper() {
        return hasMapper;
    }

    /**
     * Create a new TypeMapping that records the provided inferred mapper type.
     *
     * @param inferredMapperType the mapper ClassName to set on the returned mapping
     * @return a TypeMapping with `mapperType` set to the provided `inferredMapperType` and `hasMapper` set to true
     */
    public TypeMapping withInferredMapper(ClassName inferredMapperType) {
        return new TypeMapping(domainType, inferredMapperType, true, entityType);
    }
}