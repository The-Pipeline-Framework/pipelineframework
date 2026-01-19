package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.IndexAckDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface IndexAckMapper extends org.pipelineframework.mapper.Mapper<org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck, IndexAckDto, IndexAck> {

  IndexAckMapper INSTANCE = Mappers.getMapper( IndexAckMapper.class );

  // Domain ↔ DTO
  @Override
  IndexAckDto toDto(IndexAck entity);

  @Override
  IndexAck fromDto(IndexAckDto dto);

  // DTO ↔ gRPC
  @Override
  org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck toGrpc(IndexAckDto dto);

  @Override
  IndexAckDto fromGrpc(org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck grpc);
}