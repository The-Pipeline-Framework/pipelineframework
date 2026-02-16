package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.StoreResultDto;
import com.example.ai.sdk.entity.StoreResult;
import com.example.ai.sdk.grpc.VectorStoreSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for StoreResult conversion between gRPC, DTO, and Entity layers (outbound direction).
 */
@org.mapstruct.Mapper
public interface VectorStoreOutboundMapper extends Mapper<VectorStoreSvc.StoreResult, StoreResultDto, StoreResult> {

    VectorStoreOutboundMapper INSTANCE = Mappers.getMapper(VectorStoreOutboundMapper.class);

    /**
     * Convert a gRPC StoreResult to a StoreResultDto.
     *
     * @param grpc the gRPC StoreResult to convert
     * @return the DTO containing the same `id`, `success`, and `message` values as the gRPC object
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResultDto fromGrpc(VectorStoreSvc.StoreResult grpc);

    /**
     * Convert a StoreResult DTO to its gRPC representation.
     *
     * @param dto the DTO containing id, success, and message fields to map
     * @return a gRPC {@link VectorStoreSvc.StoreResult} with id, success, and message copied from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    VectorStoreSvc.StoreResult toGrpc(StoreResultDto dto);

    /**
     * Converts a StoreResultDto into a domain StoreResult entity.
     *
     * @param dto the data-transfer object to convert
     * @return the domain StoreResult with `id`, `success`, and `message` copied from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResult fromDto(StoreResultDto dto);

    /**
     * Converts a domain StoreResult entity into its DTO representation.
     *
     * @param domain the domain StoreResult to convert
     * @return the StoreResultDto with `id`, `success`, and `message` copied from the domain entity
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResultDto toDto(StoreResult domain);
}