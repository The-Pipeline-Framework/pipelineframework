package org.pipelineframework.restaurantapproval.common.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class ValidatedRestaurantOrderRequest extends BaseEntity implements Serializable {

  public UUID requestId;
  public String customerName;
  public String restaurantName;
  public String items;
  public BigDecimal totalAmount;
  public String currency;
  public Instant validatedAt;
}
