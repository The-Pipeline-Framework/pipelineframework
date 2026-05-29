package org.pipelineframework.restaurantapproval.validate_order_request.service;

import org.pipelineframework.restaurantapproval.common.domain.PlaceRestaurantOrderRequest;
import org.pipelineframework.restaurantapproval.common.domain.ValidatedRestaurantOrderRequest;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

@PipelineStep
@ApplicationScoped
public class ProcessValidateOrderRequestService
    implements ReactiveService<PlaceRestaurantOrderRequest, ValidatedRestaurantOrderRequest> {

  private final Clock clock = Clock.systemUTC();

  @Override
  public Uni<ValidatedRestaurantOrderRequest> process(PlaceRestaurantOrderRequest input) {
    return Uni.createFrom().item(() -> {
      Objects.requireNonNull(input, "input must not be null");
      ValidatedRestaurantOrderRequest output = new ValidatedRestaurantOrderRequest();
      output.requestId = input.requestId;
      output.customerName = input.customerName;
      output.restaurantName = input.restaurantName;
      output.items = input.items;
      output.totalAmount = input.totalAmount;
      output.currency = input.currency;
      output.validatedAt = Instant.now(clock);
      return output;
    });
  }
}
