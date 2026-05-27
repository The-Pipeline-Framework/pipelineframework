package org.pipelineframework.restaurantapproval.common.domain;

import java.io.IOException;

public interface RestaurantDecisionVisitor<R> {
  R accepted(RestaurantOrderAccepted accepted) throws IOException;

  R declined(RestaurantOrderDeclined declined) throws IOException;
}
