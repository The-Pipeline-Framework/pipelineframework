package org.pipelineframework.processor.ir;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import java.util.Optional;

/**
 * Represents one semantic directional type mapping in normalized compiler IR. It carries the actual Java representation,
 * optional application mapper information, and the canonical v3 identity when normalization supplied one.
 *
 * @param domainType the domain type for this mapping
 * @param mapperType the inferred mapper type for this mapping when one has been resolved
 * @param hasMapper whether a mapper has been inferred for this mapping
 * @param entityType the entity type used for mapper inference (the domain type that the mapper operates on)
 * @param canonicalTypeName normalized local canonical type name, when present
 */
public record TypeMapping(
        TypeName domainType,
        Optional<TypeName> mapperType,
        boolean hasMapper,
        TypeName entityType,
        Optional<String> canonicalTypeName
) {
    /**
     * Creates a new TypeMapping instance with entity type for inference.
     */
    public TypeMapping {
        if (mapperType == null || canonicalTypeName == null) {
            throw new IllegalArgumentException("Optional mapping fields must not be null; use Optional.empty()");
        }
        canonicalTypeName = canonicalTypeName.map(String::trim).filter(value -> !value.isEmpty());
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
        this(domainType, Optional.ofNullable(mapperType), hasMapper, domainType, Optional.empty());
    }

    /** Backward-compatible constructor for mappings without a normalized v3 type identity. */
    public TypeMapping(TypeName domainType, Optional<TypeName> mapperType, boolean hasMapper, TypeName entityType) {
        this(domainType, mapperType, hasMapper, entityType, Optional.empty());
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
        if (domainType == null) {
            throw new IllegalArgumentException("domainType must not be null; use unresolved() for an unavailable contract");
        }
        return new TypeMapping(domainType, Optional.empty(), false, domainType, Optional.empty());
    }

    /** Creates an unmapped Java binding for one normalized v3 canonical type. */
    public static TypeMapping canonical(TypeName domainType, String canonicalTypeName) {
        if (domainType == null) {
            throw new IllegalArgumentException("domainType must not be null");
        }
        return new TypeMapping(
            domainType, Optional.empty(), false, domainType, Optional.ofNullable(canonicalTypeName));
    }

    /** Creates an explicit mapping for a contract that is not available at extraction time. */
    public static TypeMapping unresolved() {
        return new TypeMapping(null, Optional.empty(), false, null, Optional.empty());
    }

    /**
     * Create a new TypeMapping that records the provided inferred mapper type.
     *
     * @param inferredMapperType the mapper ClassName to set on the returned mapping
     * @return a TypeMapping with `mapperType` set to the provided `inferredMapperType` and `hasMapper` set to true
     */
    public TypeMapping withInferredMapper(ClassName inferredMapperType) {
        return new TypeMapping(domainType, Optional.of(inferredMapperType), true, entityType, canonicalTypeName);
    }
}
