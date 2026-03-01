package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.dto.ParsedDocumentDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface ParsedDocumentMapper extends org.pipelineframework.mapper.Mapper<ParsedDocument, ParsedDocumentDto> {

  ParsedDocumentMapper INSTANCE = Mappers.getMapper( ParsedDocumentMapper.class );

  // Domain ↔ DTO
  ParsedDocumentDto toDto(ParsedDocument entity);

  ParsedDocument fromDto(ParsedDocumentDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument toGrpc(ParsedDocumentDto dto);

  ParsedDocumentDto fromGrpc(org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument grpc);

  @Override
  default ParsedDocument fromExternal(ParsedDocumentDto external) {
    return fromDto(external);
  }

  @Override
  default ParsedDocumentDto toExternal(ParsedDocument domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument toDtoToGrpc(ParsedDocument domain) {
    return toGrpc(toDto(domain));
  }

  default ParsedDocument fromGrpcFromDto(org.pipelineframework.search.grpc.ParseDocumentSvc.ParsedDocument grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
