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

    /**
     * Converts a gRPC LlmCompletionSvc.Prompt message into a PromptDto.
     *
     * @param grpc the gRPC Prompt message to convert
     * @return a PromptDto populated from the gRPC message's fields
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    PromptDto fromGrpc(LlmCompletionSvc.Prompt grpc);

    /**
     * Convert a PromptDto to its gRPC Prompt representation.
     *
     * @param dto the DTO containing prompt data
     * @return a gRPC `LlmCompletionSvc.Prompt` populated from the DTO's id, content, and temperature
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    LlmCompletionSvc.Prompt toGrpc(PromptDto dto);

    /**
     * Converts a PromptDto into a domain Prompt entity.
     *
     * @param dto the data transfer object containing prompt fields to map
     * @return a Prompt entity populated from the given DTO's id, content, and temperature
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    Prompt fromDto(PromptDto dto);

    /**
     * Convert a domain Prompt entity to a PromptDto.
     *
     * @param domain the domain Prompt to convert
     * @return the PromptDto with id, content, and temperature copied from the domain
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "temperature", source = "temperature")
    PromptDto toDto(Prompt domain);
}