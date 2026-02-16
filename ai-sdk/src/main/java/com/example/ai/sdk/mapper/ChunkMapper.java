package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.ChunkDto;
import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.grpc.DocumentChunkingSvc;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for Chunk conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface ChunkMapper extends Mapper<DocumentChunkingSvc.Chunk, ChunkDto, Chunk> {

    ChunkMapper INSTANCE = Mappers.getMapper(ChunkMapper.class);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    ChunkDto fromGrpc(DocumentChunkingSvc.Chunk grpc);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    DocumentChunkingSvc.Chunk toGrpc(ChunkDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    Chunk fromDto(ChunkDto dto);

    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    ChunkDto toDto(Chunk domain);
}