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

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResultDto fromGrpc(VectorStoreSvc.StoreResult grpc);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    VectorStoreSvc.StoreResult toGrpc(StoreResultDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResult fromDto(StoreResultDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "success", source = "success")
    @org.mapstruct.Mapping(target = "message", source = "message")
    StoreResultDto toDto(StoreResult domain);
}