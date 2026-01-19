package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface ParsedDocumentMapper extends org.pipelineframework.mapper.Mapper<org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument, ParsedDocumentDto, ParsedDocument> {

  ParsedDocumentMapper INSTANCE = Mappers.getMapper( ParsedDocumentMapper.class );

  // Domain ↔ DTO
  @Override
  ParsedDocumentDto toDto(ParsedDocument entity);

  @Override
  ParsedDocument fromDto(ParsedDocumentDto dto);

  // DTO ↔ gRPC
  @Override
  org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument toGrpc(ParsedDocumentDto dto);

  @Override
  ParsedDocumentDto fromGrpc(org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument grpc);
}