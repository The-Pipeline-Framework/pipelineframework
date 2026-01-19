package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.dto.CrawlRequestDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CrawlRequestMapper extends org.pipelineframework.mapper.Mapper<org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest, CrawlRequestDto, CrawlRequest> {

  CrawlRequestMapper INSTANCE = Mappers.getMapper( CrawlRequestMapper.class );

  // Domain ↔ DTO
  @Override
  CrawlRequestDto toDto(CrawlRequest entity);

  @Override
  CrawlRequest fromDto(CrawlRequestDto dto);

  // DTO ↔ gRPC
  @Override
  org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest toGrpc(CrawlRequestDto dto);

  @Override
  CrawlRequestDto fromGrpc(org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest grpc);
}