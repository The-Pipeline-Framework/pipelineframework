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
public interface VectorMapper extends Mapper<EmbeddingSvc.Vector, VectorDto, Vector> {

    VectorMapper INSTANCE = Mappers.getMapper(VectorMapper.class);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "valuesList")
    VectorDto fromGrpc(EmbeddingSvc.Vector grpc);

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

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "values")
    Vector fromDto(VectorDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "values", source = "values")
    VectorDto toDto(Vector domain);
}
