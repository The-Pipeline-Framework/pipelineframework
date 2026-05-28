package org.pipelineframework.restaurantapproval.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.restaurantapproval.common.domain.PlaceRestaurantOrderRequest;
import org.pipelineframework.restaurantapproval.common.dto.PlaceRestaurantOrderRequestDto;
import org.pipelineframework.restaurantapproval.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PlaceRestaurantOrderRequestMapper
    extends org.pipelineframework.mapper.Mapper<PlaceRestaurantOrderRequest, PlaceRestaurantOrderRequestDto> {

  PlaceRestaurantOrderRequestMapper INSTANCE = Mappers.getMapper(PlaceRestaurantOrderRequestMapper.class);

  PlaceRestaurantOrderRequestDto toDto(PlaceRestaurantOrderRequest entity);

  PlaceRestaurantOrderRequest fromDto(PlaceRestaurantOrderRequestDto dto);

  PipelineTypes.PlaceRestaurantOrderRequest toGrpc(PlaceRestaurantOrderRequestDto dto);

  PlaceRestaurantOrderRequestDto fromGrpc(PipelineTypes.PlaceRestaurantOrderRequest grpc);

  @Override
  default PlaceRestaurantOrderRequest fromExternal(PlaceRestaurantOrderRequestDto external) {
    return fromDto(external);
  }

  @Override
  default PlaceRestaurantOrderRequestDto toExternal(PlaceRestaurantOrderRequest domain) {
    return toDto(domain);
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PipelineTypes.PlaceRestaurantOrderRequest toDtoToGrpc(PlaceRestaurantOrderRequest domain) {
    return toGrpc(toDto(domain));
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  default PlaceRestaurantOrderRequest fromGrpcFromDto(PipelineTypes.PlaceRestaurantOrderRequest grpc) {
    return fromDto(fromGrpc(grpc));
  }
}
