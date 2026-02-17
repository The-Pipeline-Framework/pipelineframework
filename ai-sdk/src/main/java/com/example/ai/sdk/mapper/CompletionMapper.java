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

    /**
     * Convert a gRPC Completion message into its DTO representation.
     *
     * @param grpc the gRPC Completion message to convert
     * @return the DTO with `id`, `content`, `model`, and `timestamp` copied from the gRPC message
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    CompletionDto fromGrpc(LlmCompletionSvc.Completion grpc);

    /**
     * Convert a CompletionDto to its gRPC Completion representation.
     *
     * @param dto the source CompletionDto
     * @return the corresponding LlmCompletionSvc.Completion with id, content, model, and timestamp copied from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    LlmCompletionSvc.Completion toGrpc(CompletionDto dto);

    /**
     * Converts a CompletionDto into a domain/entity Completion.
     *
     * @param dto the data-transfer object containing completion fields
     * @return the domain/entity Completion with fields mapped from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    Completion fromDto(CompletionDto dto);

    /**
     * Convert a domain Completion entity to a CompletionDto.
     *
     * @param domain the domain/entity Completion to convert
     * @return a CompletionDto populated with `id`, `content`, `model`, and `timestamp` from the domain entity
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "model", source = "model")
    @org.mapstruct.Mapping(target = "timestamp", source = "timestamp")
    CompletionDto toDto(Completion domain);
}