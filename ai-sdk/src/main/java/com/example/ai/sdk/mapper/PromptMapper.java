package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.PromptDto;
import com.example.ai.sdk.entity.Prompt;
import com.example.ai.sdk.grpc.LlmCompletionSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Prompt conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface PromptMapper extends Mapper<LlmCompletionSvc.Prompt, PromptDto, Prompt> {

    PromptMapper INSTANCE = Mappers.getMapper(PromptMapper.class);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    PromptDto fromGrpc(LlmCompletionSvc.Prompt grpc);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    LlmCompletionSvc.Prompt toGrpc(PromptDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    Prompt fromDto(PromptDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    PromptDto toDto(Prompt domain);
}