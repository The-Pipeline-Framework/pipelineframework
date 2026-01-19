package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.dto.RawDocumentDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface RawDocumentMapper extends org.pipelineframework.mapper.Mapper<org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument, RawDocumentDto, RawDocument> {

  RawDocumentMapper INSTANCE = Mappers.getMapper( RawDocumentMapper.class );

  // Domain ↔ DTO
  @Override
  RawDocumentDto toDto(RawDocument entity);

  @Override
  RawDocument fromDto(RawDocumentDto dto);

  // DTO ↔ gRPC
  @Override
  org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument toGrpc(RawDocumentDto dto);

  @Override
  RawDocumentDto fromGrpc(org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument grpc);
}