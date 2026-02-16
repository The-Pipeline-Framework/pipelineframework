package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.StoreResultDto;
import com.example.ai.sdk.entity.StoreResult;
import com.example.ai.sdk.grpc.VectorStoreSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for StoreResult conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface StoreResultMapper extends Mapper<VectorStoreSvc.StoreResult, StoreResultDto, StoreResult> {

    StoreResultMapper INSTANCE = Mappers.getMapper(StoreResultMapper.class);

    /**
     * Create a StoreResultDto from the provided gRPC StoreResult.
     *
     * @param grpc the gRPC StoreResult to convert
     * @return the corresponding StoreResultDto with id, success, and message populated
     */
    @Override
    StoreResultDto fromGrpc(VectorStoreSvc.StoreResult grpc);

    /**
     * Convert a StoreResultDto into its gRPC StoreResult representation.
     *
     * @param dto the DTO containing id, success, and message to map
     * @return the corresponding gRPC StoreResult with id, success, and message populated
     */
    @Override
    default VectorStoreSvc.StoreResult toGrpc(StoreResultDto dto) {
        if (dto == null) {
            return null;
        }
        return VectorStoreSvc.StoreResult.newBuilder()
                .setId(dto.id())
                .setSuccess(dto.success())
                .setMessage(dto.message())
                .build();
    }

    /**
     * Convert a StoreResultDto into a domain StoreResult entity.
     *
     * @param dto the DTO representing a store operation result
     * @return the StoreResult entity with `id`, `success`, and `message` populated from the DTO
     */
    @Override
    StoreResult fromDto(StoreResultDto dto);

    /**
     * Convert a StoreResult entity to its DTO representation.
     *
     * @param domain the StoreResult entity to convert
     * @return the corresponding StoreResultDto with `id`, `success`, and `message` mapped
     */
    @Override
    StoreResultDto toDto(StoreResult domain);
}