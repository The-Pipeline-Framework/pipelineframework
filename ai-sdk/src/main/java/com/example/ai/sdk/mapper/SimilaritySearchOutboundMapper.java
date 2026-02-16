package com.example.ai.sdk.mapper;

import com.example.ai.sdk.dto.ScoredChunkDto;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.grpc.DocumentChunkingSvc;
import com.example.ai.sdk.grpc.SimilaritySearchSvc;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.mapper.Mapper;

/**
 * MapStruct mapper for ScoredChunk conversion between gRPC, DTO, and Entity layers (outbound direction).
 */
@org.mapstruct.Mapper(uses = {ChunkMapper.class})
public interface SimilaritySearchOutboundMapper extends Mapper<SimilaritySearchSvc.ScoredChunk, ScoredChunkDto, ScoredChunk> {

    SimilaritySearchOutboundMapper INSTANCE = Mappers.getMapper(SimilaritySearchOutboundMapper.class);

    /**
     * Convert a gRPC ScoredChunk message to a ScoredChunkDto.
     *
     * @param grpc the source gRPC ScoredChunk to convert
     * @return the ScoredChunkDto with `chunk` and `score` mapped from the source
     */
    @Override
    ScoredChunkDto fromGrpc(SimilaritySearchSvc.ScoredChunk grpc);

    /**
     * Convert the provided DTO into its corresponding gRPC representation.
     *
     * @param dto the DTO to convert
     * @return the corresponding gRPC ScoredChunk
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
     * Converts a ScoredChunkDto into its domain entity representation.
     *
     * @param dto the DTO containing the chunk content and score to map
     * @return the domain ScoredChunk with its chunk and score populated from the dto
     */
    @Override
    ScoredChunk fromDto(ScoredChunkDto dto);

    /**
     * Converts a domain ScoredChunk entity into its DTO representation.
     *
     * @param domain the domain ScoredChunk entity to convert
     * @return a ScoredChunkDto with the `chunk` and `score` fields copied from the domain entity
     */
    @Override
    ScoredChunkDto toDto(ScoredChunk domain);
}