package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.ScoredChunkDto;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.grpc.SimilaritySearchSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for ScoredChunk conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface ScoredChunkMapper extends Mapper<SimilaritySearchSvc.ScoredChunk, ScoredChunkDto, ScoredChunk> {

    ScoredChunkMapper INSTANCE = Mappers.getMapper(ScoredChunkMapper.class);

    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunkDto fromGrpc(SimilaritySearchSvc.ScoredChunk grpc);

    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    SimilaritySearchSvc.ScoredChunk toGrpc(ScoredChunkDto dto);

    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunk fromDto(ScoredChunkDto dto);

    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunkDto toDto(ScoredChunk domain);
}