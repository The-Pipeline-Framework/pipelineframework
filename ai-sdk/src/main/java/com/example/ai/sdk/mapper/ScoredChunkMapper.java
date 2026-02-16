package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.ScoredChunkDto;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.grpc.DocumentChunkingSvc;
import com.example.ai.sdk.grpc.SimilaritySearchSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for ScoredChunk conversion between gRPC, DTO, and Entity layers.
 */
@org.mapstruct.Mapper
public interface ScoredChunkMapper extends Mapper<SimilaritySearchSvc.ScoredChunk, ScoredChunkDto, ScoredChunk> {

    ScoredChunkMapper INSTANCE = Mappers.getMapper(ScoredChunkMapper.class);

    /**
     * Converts a gRPC ScoredChunk message to a ScoredChunkDto.
     *
     * @param grpc the gRPC ScoredChunk to convert
     * @return a ScoredChunkDto with the corresponding chunk and score from the gRPC message
     */
    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunkDto fromGrpc(SimilaritySearchSvc.ScoredChunk grpc);

    /**
     * Converts a scored-chunk DTO into the corresponding gRPC ScoredChunk message.
     *
     * @param dto the DTO containing the chunk content and its similarity score
     * @return a gRPC ScoredChunk with `chunk` and `score` populated from the DTO
     */
    @Override
    default SimilaritySearchSvc.ScoredChunk toGrpc(ScoredChunkDto dto) {
        if (dto == null) {
            return null;
        }
        DocumentChunkingSvc.Chunk grpcChunk = ChunkMapper.INSTANCE.toGrpc(dto.chunk());
        return SimilaritySearchSvc.ScoredChunk.newBuilder()
                .setChunk(grpcChunk)
                .setScore(dto.score())
                .build();
    }

    /**
     * Converts a ScoredChunkDto to a ScoredChunk entity.
     *
     * @param dto the data transfer object containing chunk and score values
     * @return the entity populated with values from {@code dto}
     */
    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunk fromDto(ScoredChunkDto dto);

    /**
     * Converts a domain ScoredChunk entity to a ScoredChunkDto.
     *
     * @param domain the domain entity whose fields are mapped into the DTO
     * @return a ScoredChunkDto with `chunk` and `score` copied from the domain entity
     */
    @Override
    @org.mapstruct.Mapping(target = "chunk", source = "chunk")
    @org.mapstruct.Mapping(target = "score", source = "score")
    ScoredChunkDto toDto(ScoredChunk domain);
}