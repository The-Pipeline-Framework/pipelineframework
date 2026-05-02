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
@org.mapstruct.Mapper(uses = {ChunkMapper.class})
public interface ScoredChunkMapper extends Mapper<ScoredChunk, SimilaritySearchSvc.ScoredChunk> {

    ScoredChunkMapper INSTANCE = Mappers.getMapper(ScoredChunkMapper.class);

    @Override
    default ScoredChunk fromExternal(SimilaritySearchSvc.ScoredChunk external) {
        return fromDto(fromGrpc(external));
    }

    @Override
    default SimilaritySearchSvc.ScoredChunk toExternal(ScoredChunk domain) {
        return toGrpc(toDto(domain));
    }

    /**
     * Converts a gRPC ScoredChunk message to a ScoredChunkDto.
     *
     * @param grpc the gRPC ScoredChunk to convert
     * @return a ScoredChunkDto with the corresponding chunk and score from the gRPC message
     */
    ScoredChunkDto fromGrpc(SimilaritySearchSvc.ScoredChunk grpc);

    /**
     * Converts a scored-chunk DTO into the corresponding gRPC ScoredChunk message.
     *
     * @param dto the DTO containing the chunk content and its similarity score
     * @return a gRPC ScoredChunk with `chunk` and `score` populated from the DTO
     */
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
    ScoredChunk fromDto(ScoredChunkDto dto);

    /**
     * Converts a domain ScoredChunk entity to a ScoredChunkDto.
     *
     * @param domain the domain entity whose fields are mapped into the DTO
     * @return a ScoredChunkDto with `chunk` and `score` copied from the domain entity
     */
    ScoredChunkDto toDto(ScoredChunk domain);
}
