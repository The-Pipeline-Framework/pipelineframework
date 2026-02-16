package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.VectorDto;
import com.example.ai.sdk.entity.Vector;
import com.example.ai.sdk.grpc.EmbeddingSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Vector conversion between gRPC, DTO, and Entity layers (outbound direction).
 */
@org.mapstruct.Mapper
public interface EmbeddingOutboundMapper extends Mapper<EmbeddingSvc.Vector, VectorDto, Vector> {

    EmbeddingOutboundMapper INSTANCE = Mappers.getMapper(EmbeddingOutboundMapper.class);

    /**
     * Convert a gRPC EmbeddingSvc.Vector to a VectorDto.
     *
     * @param grpc the gRPC vector whose fields will be mapped
     * @return a VectorDto with `id` taken from `grpc.id` and `values` taken from `grpc.valuesList`
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "valuesList")
    VectorDto fromGrpc(EmbeddingSvc.Vector grpc);

    /**
     * Convert a VectorDto to a gRPC EmbeddingSvc.Vector.
     *
     * @param dto the source DTO to convert; may be {@code null}
     * @return the corresponding {@link EmbeddingSvc.Vector}, or {@code null} if {@code dto} is {@code null}
     */
    @Override
    default EmbeddingSvc.Vector toGrpc(VectorDto dto) {
        if (dto == null) {
            return null;
        }
        EmbeddingSvc.Vector.Builder builder = EmbeddingSvc.Vector.newBuilder()
                .setId(dto.id());
        if (dto.values() != null) {
            builder.addAllValues(dto.values());
        }
        return builder.build();
    }

    /**
     * Convert a VectorDto into a Vector domain entity.
     *
     * Maps the DTO's `id` and `values` fields to the corresponding fields on the returned domain Vector.
     *
     * @param dto the source data transfer object
     * @return the domain Vector populated from the given DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "values")
    Vector fromDto(VectorDto dto);

    /**
     * Convert a domain Vector entity to a VectorDto.
     *
     * @param domain the domain Vector entity to convert
     * @return the VectorDto with id and values taken from the domain entity
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "values")
    VectorDto toDto(Vector domain);
}