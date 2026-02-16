package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.StoreResultDto;
import com.example.ai.sdk.entity.StoreResult;
import com.example.ai.sdk.grpc.VectorStoreSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for StoreResult conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface StoreResultMapper extends Mapper<VectorStoreSvc.StoreResult, StoreResultDto, StoreResult> {

    StoreResultMapper INSTANCE = Mappers.getMapper(StoreResultMapper.class);

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