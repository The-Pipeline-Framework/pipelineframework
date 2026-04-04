package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.BeforeMapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface TokenBatchMapper extends org.pipelineframework.mapper.Mapper<TokenBatch, TokenBatchDto> {

  TokenBatchMapper INSTANCE = Mappers.getMapper( TokenBatchMapper.class );

  // Domain ↔ DTO
  TokenBatchDto toDto(TokenBatch entity);

  TokenBatch fromDto(TokenBatchDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.PipelineTypes.TokenBatch toGrpc(TokenBatchDto dto);

  TokenBatchDto fromGrpc(org.pipelineframework.search.grpc.PipelineTypes.TokenBatch grpc);

  @BeforeMapping
  default void validateTokenBatchDto(TokenBatchDto dto) {
    if (dto == null) {
      return;
    }
    if (dto.getBatchIndex() == null) {
      throw new IllegalArgumentException("TokenBatchDto.batchIndex must not be null");
    }
    if (dto.getBatchIndex() < 0) {
      throw new IllegalArgumentException("TokenBatchDto.batchIndex must be >= 0");
    }
    if (dto.getTokenCount() == null) {
      throw new IllegalArgumentException("TokenBatchDto.tokenCount must not be null");
    }
    if (dto.getTokenCount() <= 0) {
      throw new IllegalArgumentException("TokenBatchDto.tokenCount must be > 0");
    }
  }

  @Override
  default TokenBatch fromExternal(TokenBatchDto external) {
    return fromDto(external);
  }

  @Override
  default TokenBatchDto toExternal(TokenBatch domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.PipelineTypes.TokenBatch toDtoToGrpc(TokenBatch domain) {
    return toGrpc(toDto(domain));
  }

  default TokenBatch fromGrpcFromDto(org.pipelineframework.search.grpc.PipelineTypes.TokenBatch grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
