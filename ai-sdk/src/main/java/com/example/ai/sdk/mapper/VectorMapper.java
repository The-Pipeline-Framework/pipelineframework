package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.VectorDto;
import com.example.ai.sdk.entity.Vector;
import com.example.ai.sdk.grpc.EmbeddingSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Vector conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface VectorMapper extends Mapper<Vector, EmbeddingSvc.Vector> {

    VectorMapper INSTANCE = Mappers.getMapper(VectorMapper.class);

    @Override
    default Vector fromExternal(EmbeddingSvc.Vector external) {
        return fromDto(fromGrpc(external));
    }

    @Override
    default EmbeddingSvc.Vector toExternal(Vector domain) {
        return toGrpc(toDto(domain));
    }

    /**
     * Convert a gRPC EmbeddingSvc.Vector to a VectorDto.
     *
     * @param grpc the gRPC EmbeddingSvc.Vector to convert
     * @return the mapped VectorDto whose `id` is taken from the gRPC vector's id and whose `values` are taken from the gRPC vector's valuesList
     */
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "valuesList")
    VectorDto fromGrpc(EmbeddingSvc.Vector grpc);

    /**
     * Converts a VectorDto into a gRPC EmbeddingSvc.Vector.
     *
     * @param dto the source DTO; its `id` and `values` (if non-null) are copied into the resulting gRPC vector, may be `null`
     * @return the corresponding {@code EmbeddingSvc.Vector}, or `null` if {@code dto} is `null`
     */
    default EmbeddingSvc.Vector toGrpc(VectorDto dto) {
        if (dto == null) {
            return null;
        }
        EmbeddingSvc.Vector.Builder builder = EmbeddingSvc.Vector.newBuilder();
        if (dto.id() != null) {
            builder.setId(dto.id());
        }
        if (dto.values() != null) {
            builder.addAllValues(dto.values());
        }
        return builder.build();
    }

    /**
     * Convert a VectorDto to a Vector domain entity.
     *
     * @param dto the source DTO containing id and values
     * @return the domain Vector with id and values copied from the DTO
     */
    Vector fromDto(VectorDto dto);

    /**
     * Creates a VectorDto from the given domain Vector.
     *
     * @param domain the Vector domain entity to convert
     * @return a VectorDto whose id and values are copied from the domain entity
     */
    VectorDto toDto(Vector domain);
}
