package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.StoreResultDto;
import com.example.ai.sdk.entity.StoreResult;
import com.example.ai.sdk.grpc.VectorStoreSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for StoreResult conversion between gRPC, DTO, and Entity layers (outbound direction).
 */
@org.mapstruct.Mapper
public interface VectorStoreOutboundMapper extends Mapper<StoreResult, VectorStoreSvc.StoreResult> {

    VectorStoreOutboundMapper INSTANCE = Mappers.getMapper(VectorStoreOutboundMapper.class);

    @Override
    default StoreResult fromExternal(VectorStoreSvc.StoreResult external) {
        return fromDto(fromGrpc(external));
    }

    @Override
    default VectorStoreSvc.StoreResult toExternal(StoreResult domain) {
        return toGrpc(toDto(domain));
    }

    /**
     * Convert a gRPC StoreResult to a StoreResultDto.
     *
     * @param grpc the gRPC StoreResult to convert
     * @return the DTO containing the same `id`, `success`, and `message` values as the gRPC object
     */
    StoreResultDto fromGrpc(VectorStoreSvc.StoreResult grpc);

    /**
     * Convert a StoreResult DTO to its gRPC representation.
     *
     * @param dto the DTO containing id, success, and message fields to map
     * @return a gRPC {@link VectorStoreSvc.StoreResult} with id, success, and message copied from the DTO
     */
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
     * Converts a StoreResultDto into a domain StoreResult entity.
     *
     * @param dto the data-transfer object to convert
     * @return the domain StoreResult with `id`, `success`, and `message` copied from the DTO
     */
    StoreResult fromDto(StoreResultDto dto);

    /**
     * Converts a domain StoreResult entity into its DTO representation.
     *
     * @param domain the domain StoreResult to convert
     * @return the StoreResultDto with `id`, `success`, and `message` copied from the domain entity
     */
    StoreResultDto toDto(StoreResult domain);
}
