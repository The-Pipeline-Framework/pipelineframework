package org.pipelineframework.restaurantapproval.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.util.UUID;

@JsonSerialize(using = RestaurantDecisionDtoJsonSerializer.class)
@JsonDeserialize(using = RestaurantDecisionDtoJsonDeserializer.class)
public sealed interface RestaurantDecisionDto extends Serializable
    permits RestaurantOrderAcceptedDto, RestaurantOrderDeclinedDto {

  UUID orderId();
}
