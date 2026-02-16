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

    /**
     * Convert a gRPC Chunk message to a DTO representation.
     *
     * @param grpc the gRPC Chunk message to convert
     * @return the DTO populated with `id`, `documentId`, `content`, and `position` from the gRPC message
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    ChunkDto fromGrpc(DocumentChunkingSvc.Chunk grpc);

    /**
     * Convert a ChunkDto to its gRPC DocumentChunkingSvc.Chunk representation.
     *
     * @param dto the DTO containing chunk data to convert
     * @return a {@link DocumentChunkingSvc.Chunk} populated with id, documentId, content, and position from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    DocumentChunkingSvc.Chunk toGrpc(ChunkDto dto);

    /**
     * Maps a ChunkDto to its corresponding domain Chunk entity.
     *
     * @param dto the data transfer object containing id, documentId, content, and position
     * @return a Chunk domain instance with id, documentId, content, and position copied from the DTO
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    Chunk fromDto(ChunkDto dto);

    /**
     * Converts a domain Chunk entity into a ChunkDto.
     *
     * @param domain the domain Chunk entity to convert
     * @return a ChunkDto representing the provided domain Chunk
     */
    @Override
    @org.mapstruct.Mapping(target = "id", source = "id")
    @org.mapstruct.Mapping(target = "documentId", source = "documentId")
    @org.mapstruct.Mapping(target = "content", source = "content")
    @org.mapstruct.Mapping(target = "position", source = "position")
    ChunkDto toDto(Chunk domain);
}