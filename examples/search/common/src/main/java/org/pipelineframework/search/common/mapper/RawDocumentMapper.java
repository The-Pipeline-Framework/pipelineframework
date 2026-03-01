package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.dto.RawDocumentDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface RawDocumentMapper extends org.pipelineframework.mapper.Mapper<RawDocument, RawDocumentDto> {

  RawDocumentMapper INSTANCE = Mappers.getMapper( RawDocumentMapper.class );

  // Domain ↔ DTO
  RawDocumentDto toDto(RawDocument entity);

  RawDocument fromDto(RawDocumentDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument toGrpc(RawDocumentDto dto);

  RawDocumentDto fromGrpc(org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument grpc);

  @Override
  default RawDocument fromExternal(RawDocumentDto external) {
    return fromDto(external);
  }

  @Override
  default RawDocumentDto toExternal(RawDocument domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument toDtoToGrpc(RawDocument domain) {
    return toGrpc(toDto(domain));
  }

  default RawDocument fromGrpcFromDto(org.pipelineframework.search.grpc.CrawlSourceSvc.RawDocument grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
