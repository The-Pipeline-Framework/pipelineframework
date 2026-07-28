package org.pipelineframework.processor.ir;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import java.util.Optional;

/**
 * Represents a semantic directional type mapping derived from annotations. Contains semantic information from the @PipelineStep
 * annotation, including domain types and inferred mapper information.
 *
 * @param domainType the domain type for this mapping
 * @param mapperType the inferred mapper type for this mapping when one has been resolved
 * @param hasMapper whether a mapper has been inferred for this mapping
 * @param entityType the entity type used for mapper inference (the domain type that the mapper operates on)
 */
public record TypeMapping(
        TypeName domainType,
        Optional<TypeName> mapperType,
        boolean hasMapper,
        TypeName entityType
) {
    /**
     * Creates a new TypeMapping instance with entity type for inference.
     */
    public TypeMapping {
        mapperType = mapperType == null ? Optional.empty() : mapperType;
        // entityType defaults to domainType if not specified
        if (entityType == null) {
            entityType = domainType;
        }
    }

    /**
     * Backward-compatible constructor that creates a TypeMapping and defaults the entityType to the provided domainType.
     *
     * @param domainType the domain type for this mapping; used as the entityType when none is provided
     * @param mapperType the mapper type, or null if not specified by a legacy caller
     * @param hasMapper  true if a mapper has been inferred, false otherwise
     */
    public TypeMapping(TypeName domainType, TypeName mapperType, boolean hasMapper) {
        this(domainType, Optional.ofNullable(mapperType), hasMapper, domainType);
    }

    /**
     * Creates a mapping with a domain contract and no mapper metadata.
     *
     * <p>This is the explicit representation for boundaries whose contract is
     * resolved without an application mapper.</p>
     *
     * @param domainType the domain type for the mapping
     * @return a mapping with no inferred mapper
     */
    public static TypeMapping withoutMapper(TypeName domainType) {
        return new TypeMapping(domainType, Optional.empty(), false, domainType);
    }

    /**
     * Create a new TypeMapping that records the provided inferred mapper type.
     *
     * @param inferredMapperType the mapper ClassName to set on the returned mapping
     * @return a TypeMapping with `mapperType` set to the provided `inferredMapperType` and `hasMapper` set to true
     */
    public TypeMapping withInferredMapper(ClassName inferredMapperType) {
        return new TypeMapping(domainType, Optional.of(inferredMapperType), true, entityType);
    }
}
