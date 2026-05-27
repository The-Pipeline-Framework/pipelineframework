package org.pipelineframework.restaurantapproval.common.domain;

public interface RestaurantDecisionVisitor<R> {
  R accepted(RestaurantOrderAccepted accepted);

  R declined(RestaurantOrderDeclined declined);
}
