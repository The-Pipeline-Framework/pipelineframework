package org.pipelineframework.restaurantapproval.common.domain;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class TerminalOrderState extends BaseEntity implements Serializable {

  public UUID orderId;
  public String outcome;
  public Instant resolvedAt;
  public String restaurantStatus;
  public String summary;
}
