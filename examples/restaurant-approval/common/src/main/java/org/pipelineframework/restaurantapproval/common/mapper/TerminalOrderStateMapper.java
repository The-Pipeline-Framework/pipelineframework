package org.pipelineframework.restaurantapproval.common.mapper;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.restaurantapproval.common.domain.TerminalOrderState;
import org.pipelineframework.restaurantapproval.common.dto.TerminalOrderStateDto;
import org.pipelineframework.restaurantapproval.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface TerminalOrderStateMapper
    extends org.pipelineframework.mapper.Mapper<TerminalOrderState, TerminalOrderStateDto> {

  TerminalOrderStateMapper INSTANCE = Mappers.getMapper(TerminalOrderStateMapper.class);

  TerminalOrderStateDto toDto(TerminalOrderState entity);

  TerminalOrderState fromDto(TerminalOrderStateDto dto);

  @BeanMapping(unmappedTargetPolicy = ReportingPolicy.IGNORE)
  PipelineTypes.TerminalOrderState toGrpc(TerminalOrderStateDto dto);

  @Mapping(target = "id", ignore = true)
  TerminalOrderStateDto fromGrpc(PipelineTypes.TerminalOrderState grpc);

  @Override
  default TerminalOrderState fromExternal(TerminalOrderStateDto external) {
    return fromDto(external);
  }

  @Override
  default TerminalOrderStateDto toExternal(TerminalOrderState domain) {
    return toDto(domain);
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PipelineTypes.TerminalOrderState toDtoToGrpc(TerminalOrderState domain) {
    return toGrpc(toDto(domain));
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default TerminalOrderState fromGrpcFromDto(PipelineTypes.TerminalOrderState grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
