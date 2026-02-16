package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.CompletionDto;
import com.example.ai.sdk.entity.Completion;
import com.example.ai.sdk.grpc.LlmCompletionSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Completion conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface CompletionMapper extends Mapper<LlmCompletionSvc.Completion, CompletionDto, Completion> {

    CompletionMapper INSTANCE = Mappers.getMapper(CompletionMapper.class);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    CompletionDto fromGrpc(LlmCompletionSvc.Completion grpc);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    LlmCompletionSvc.Completion toGrpc(CompletionDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    Completion fromDto(CompletionDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    CompletionDto toDto(Completion domain);
}