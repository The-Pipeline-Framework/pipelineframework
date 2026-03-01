package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.dto.CrawlRequestDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CrawlRequestMapper extends org.pipelineframework.mapper.Mapper<CrawlRequest, CrawlRequestDto> {

  CrawlRequestMapper INSTANCE = Mappers.getMapper( CrawlRequestMapper.class );

  // Domain ↔ DTO
  CrawlRequestDto toDto(CrawlRequest entity);

  CrawlRequest fromDto(CrawlRequestDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest toGrpc(CrawlRequestDto dto);

  CrawlRequestDto fromGrpc(org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest grpc);

  @Override
  default CrawlRequest fromExternal(CrawlRequestDto external) {
    return fromDto(external);
  }

  @Override
  default CrawlRequestDto toExternal(CrawlRequest domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest toDtoToGrpc(CrawlRequest domain) {
    return toGrpc(toDto(domain));
  }

  default CrawlRequest fromGrpcFromDto(org.pipelineframework.search.grpc.CrawlSourceSvc.CrawlRequest grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
