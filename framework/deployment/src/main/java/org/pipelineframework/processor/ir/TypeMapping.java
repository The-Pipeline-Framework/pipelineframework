package org.pipelineframework.processor.ir;

import com.squareup.javapoet.TypeName;
import lombok.Getter;

/**
 * Represents a semantic directional type mapping derived from annotations. Contains semantic information from the @PipelineStep
 * annotation, including domain types and mapper information.
 *
 * @param domainType Gets the domain type for this mapping.
 * @param mapperType Gets the mapper type for this mapping.
 * @param hasMapper Whether a mapper is present for this mapping
 */
public record TypeMapping(
        @Getter TypeName domainType,
        @Getter TypeName mapperType,
        boolean hasMapper
) {
    /**
     * Creates a new TypeMapping instance.
     */
    public TypeMapping {}

    /**
     * Checks if this mapping has an associated mapper.
     *
     * @return true if a mapper is present, false otherwise
     */
    @Override
    public boolean hasMapper() {
        return hasMapper;
    }
}
