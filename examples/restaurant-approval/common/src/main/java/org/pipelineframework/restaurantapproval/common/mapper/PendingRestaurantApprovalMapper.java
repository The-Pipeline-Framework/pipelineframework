package org.pipelineframework.restaurantapproval.common.mapper;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.restaurantapproval.common.domain.PendingRestaurantApproval;
import org.pipelineframework.restaurantapproval.common.dto.PendingRestaurantApprovalDto;
import org.pipelineframework.restaurantapproval.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PendingRestaurantApprovalMapper
    extends org.pipelineframework.mapper.Mapper<PendingRestaurantApproval, PendingRestaurantApprovalDto> {

  PendingRestaurantApprovalMapper INSTANCE = Mappers.getMapper(PendingRestaurantApprovalMapper.class);

  PendingRestaurantApprovalDto toDto(PendingRestaurantApproval entity);

  PendingRestaurantApproval fromDto(PendingRestaurantApprovalDto dto);

  @BeanMapping(unmappedTargetPolicy = ReportingPolicy.IGNORE)
  PipelineTypes.PendingRestaurantApproval toGrpc(PendingRestaurantApprovalDto dto);

  @Mapping(target = "id", ignore = true)
  PendingRestaurantApprovalDto fromGrpc(PipelineTypes.PendingRestaurantApproval grpc);

  @Override
  default PendingRestaurantApproval fromExternal(PendingRestaurantApprovalDto external) {
    return fromDto(external);
  }

  @Override
  default PendingRestaurantApprovalDto toExternal(PendingRestaurantApproval domain) {
    return toDto(domain);
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PipelineTypes.PendingRestaurantApproval toDtoToGrpc(PendingRestaurantApproval domain) {
    return toGrpc(toDto(domain));
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PendingRestaurantApproval fromGrpcFromDto(PipelineTypes.PendingRestaurantApproval grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
