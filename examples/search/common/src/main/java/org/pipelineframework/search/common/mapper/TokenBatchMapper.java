package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.TokenBatch;
import org.pipelineframework.search.common.dto.TokenBatchDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface TokenBatchMapper extends org.pipelineframework.mapper.Mapper<org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch, TokenBatchDto, TokenBatch> {

  TokenBatchMapper INSTANCE = Mappers.getMapper( TokenBatchMapper.class );

  // Domain ↔ DTO
  @Override
  TokenBatchDto toDto(TokenBatch entity);

  @Override
  TokenBatch fromDto(TokenBatchDto dto);

  // DTO ↔ gRPC
  @Override
  org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch toGrpc(TokenBatchDto dto);

  @Override
  TokenBatchDto fromGrpc(org.pipelineframework.search.grpc.TokenizeContentSvc.TokenBatch grpc);
}