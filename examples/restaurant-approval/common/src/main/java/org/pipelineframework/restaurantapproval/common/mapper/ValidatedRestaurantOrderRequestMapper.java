package org.pipelineframework.restaurantapproval.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.restaurantapproval.common.domain.ValidatedRestaurantOrderRequest;
import org.pipelineframework.restaurantapproval.common.dto.ValidatedRestaurantOrderRequestDto;
import org.pipelineframework.restaurantapproval.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface ValidatedRestaurantOrderRequestMapper
    extends org.pipelineframework.mapper.Mapper<ValidatedRestaurantOrderRequest, ValidatedRestaurantOrderRequestDto> {

  ValidatedRestaurantOrderRequestMapper INSTANCE = Mappers.getMapper(ValidatedRestaurantOrderRequestMapper.class);

  ValidatedRestaurantOrderRequestDto toDto(ValidatedRestaurantOrderRequest entity);

  ValidatedRestaurantOrderRequest fromDto(ValidatedRestaurantOrderRequestDto dto);

  PipelineTypes.ValidatedRestaurantOrderRequest toGrpc(ValidatedRestaurantOrderRequestDto dto);

  ValidatedRestaurantOrderRequestDto fromGrpc(PipelineTypes.ValidatedRestaurantOrderRequest grpc);

  @Override
  default ValidatedRestaurantOrderRequest fromExternal(ValidatedRestaurantOrderRequestDto external) {
    return fromDto(external);
  }

  @Override
  default ValidatedRestaurantOrderRequestDto toExternal(ValidatedRestaurantOrderRequest domain) {
    return toDto(domain);
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PipelineTypes.ValidatedRestaurantOrderRequest toDtoToGrpc(ValidatedRestaurantOrderRequest domain) {
    return toGrpc(toDto(domain));
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default ValidatedRestaurantOrderRequest fromGrpcFromDto(PipelineTypes.ValidatedRestaurantOrderRequest grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
