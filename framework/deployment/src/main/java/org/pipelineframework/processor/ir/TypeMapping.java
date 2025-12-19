package org.pipelineframework.processor.ir;

import com.squareup.javapoet.TypeName;

/**
 * Represents a semantic directional type mapping derived from annotations.
 * Contains semantic information from the @PipelineStep annotation, including both
 * domain types and corresponding gRPC message types.
 */
public class TypeMapping {
    private final TypeName domainType;
    private final TypeName grpcType;  // This is semantic - specified by user in annotation  
    private final TypeName mapperType;
    private final boolean hasMapper;

    /**
     * Creates a new TypeMapping instance.
     * 
     * @param domainType the domain type specified in the annotation
     * @param grpcType the gRPC message type specified in the annotation, may be null
     * @param mapperType the mapper class specified in the annotation, may be null
     * @param hasMapper whether a mapper is present for this mapping
     */
    public TypeMapping(TypeName domainType, TypeName grpcType, TypeName mapperType, boolean hasMapper) {
        this.domainType = domainType;
        this.grpcType = grpcType;
        this.mapperType = mapperType;
        this.hasMapper = hasMapper;
    }

    /**
     * Gets the domain type for this mapping.
     * 
     * @return the domain type
     */
    public TypeName getDomainType() {
        return domainType;
    }

    /**
     * Gets the gRPC message type for this mapping.
     * This represents semantic configuration specifying which gRPC message should be used.
     * 
     * @return the gRPC message type
     */
    public TypeName getGrpcType() {
        return grpcType;
    }

    /**
     * Gets the mapper type for this mapping.
     * 
     * @return the mapper type
     */
    public TypeName getMapperType() {
        return mapperType;
    }

    /**
     * Checks if this mapping has an associated mapper.
     * 
     * @return true if a mapper is present, false otherwise
     */
    public boolean hasMapper() {
        return hasMapper;
    }
}