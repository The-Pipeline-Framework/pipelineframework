package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
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

  @Mapping(target = "batchIndex", ignore = true)
  TokenBatch fromDto(TokenBatchDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch toGrpc(TokenBatchDto dto);

  TokenBatchDto fromGrpc(org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch grpc);

  @Override
  default TokenBatch fromExternal(TokenBatchDto external) {
    return fromDto(external);
  }

  @Override
  default TokenBatchDto toExternal(TokenBatch domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch toDtoToGrpc(TokenBatch domain) {
    return toGrpc(toDto(domain));
  }

  default TokenBatch fromGrpcFromDto(org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
