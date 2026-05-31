package org.pipelineframework.search.common.mapper;

import org.mapstruct.BeanMapping;
import org.mapstruct.BeforeMapping;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.EmbeddedChunk;
import org.pipelineframework.search.common.dto.EmbeddedChunkDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface EmbeddedChunkMapper extends org.pipelineframework.mapper.Mapper<EmbeddedChunk, EmbeddedChunkDto> {

  EmbeddedChunkMapper INSTANCE = Mappers.getMapper(EmbeddedChunkMapper.class);

  EmbeddedChunkDto toDto(EmbeddedChunk entity);

  EmbeddedChunk fromDto(EmbeddedChunkDto dto);

  @BeanMapping(unmappedTargetPolicy = ReportingPolicy.IGNORE)
  org.pipelineframework.search.grpc.PipelineTypes.EmbeddedChunk toGrpc(EmbeddedChunkDto dto);

  EmbeddedChunkDto fromGrpc(org.pipelineframework.search.grpc.PipelineTypes.EmbeddedChunk grpc);

  @BeforeMapping
  default void validateEmbeddedChunkDto(EmbeddedChunkDto dto) {
    if (dto == null) {
      return;
    }
    if (dto.getBatchIndex() == null) {
      throw new IllegalArgumentException("EmbeddedChunkDto.batchIndex must not be null");
    }
    if (dto.getBatchIndex() < 0) {
      throw new IllegalArgumentException("EmbeddedChunkDto.batchIndex must be >= 0");
    }
    if (dto.getTokenCount() == null) {
      throw new IllegalArgumentException("EmbeddedChunkDto.tokenCount must not be null");
    }
    if (dto.getTokenCount() <= 0) {
      throw new IllegalArgumentException("EmbeddedChunkDto.tokenCount must be > 0");
    }
    if (dto.getVectorHash() == null || dto.getVectorHash().isBlank()) {
      throw new IllegalArgumentException("EmbeddedChunkDto.vectorHash must not be blank");
    }
  }

  @Override
  default EmbeddedChunk fromExternal(EmbeddedChunkDto external) {
    return fromDto(external);
  }

  @Override
  default EmbeddedChunkDto toExternal(EmbeddedChunk domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.PipelineTypes.EmbeddedChunk toDtoToGrpc(EmbeddedChunk domain) {
    return toGrpc(toDto(domain));
  }

  default EmbeddedChunk fromGrpcFromDto(org.pipelineframework.search.grpc.PipelineTypes.EmbeddedChunk grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
