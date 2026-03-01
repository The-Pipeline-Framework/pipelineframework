package org.pipelineframework.search.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.search.common.domain.IndexAck;
import org.pipelineframework.search.common.dto.IndexAckDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "cdi",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface IndexAckMapper extends org.pipelineframework.mapper.Mapper<IndexAck, IndexAckDto> {

  IndexAckMapper INSTANCE = Mappers.getMapper( IndexAckMapper.class );

  // Domain ↔ DTO
  IndexAckDto toDto(IndexAck entity);

  IndexAck fromDto(IndexAckDto dto);

  // DTO ↔ gRPC
  org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck toGrpc(IndexAckDto dto);

  IndexAckDto fromGrpc(org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck grpc);

  @Override
  default IndexAck fromExternal(IndexAckDto external) {
    return fromDto(external);
  }

  @Override
  default IndexAckDto toExternal(IndexAck domain) {
    return toDto(domain);
  }

  default org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck toDtoToGrpc(IndexAck domain) {
    return toGrpc(toDto(domain));
  }

  default IndexAck fromGrpcFromDto(org.pipelineframework.search.grpc.IndexDocumentSvc.IndexAck grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
