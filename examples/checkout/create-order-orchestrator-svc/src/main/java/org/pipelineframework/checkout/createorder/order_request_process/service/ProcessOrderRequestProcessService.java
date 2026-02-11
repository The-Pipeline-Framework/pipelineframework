package org.pipelineframework.checkout.createorder.order_request_process.service;

import org.pipelineframework.checkout.createorder.common.domain.OrderRequest;
import org.pipelineframework.checkout.createorder.common.domain.OrderLineItem;
import org.pipelineframework.annotation.PipelineStep;
import jakarta.enterprise.context.ApplicationScoped;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = OrderRequest.class,
    outputType = OrderLineItem.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.checkout.createorder.common.mapper.OrderRequestMapper.class,
    outboundMapper = org.pipelineframework.checkout.createorder.common.mapper.OrderLineItemMapper.class
)
@ApplicationScoped
public class ProcessOrderRequestProcessService
    implements ReactiveService<OrderRequest, OrderLineItem> {

  private static final Logger LOG = Logger.getLogger(ProcessOrderRequestProcessService.class);

  @Override
  public Uni<OrderLineItem> process(OrderRequest input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
    }
    String raw = input.items() == null ? "" : input.items().trim();
    String first = raw.isEmpty() ? "" : raw.split(",")[0];
    String[] parts = first.trim().split(":");
    String sku = !parts[0].isBlank() ? parts[0].trim() : "unknown-sku";
    int quantity = parts.length > 1 ? parseQuantity(parts[1]) : 1;
    OrderLineItem output = new OrderLineItem(input.requestId(), input.customerId(), sku, quantity);
    return Uni.createFrom().item(output);
  }

  private static int parseQuantity(String value) {
    if (value == null) {
      LOG.warn("Invalid quantity value <null>; defaulting to 1");
      return 1;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return parsed <= 0 ? 1 : parsed;
    } catch (NumberFormatException e) {
      LOG.warnf("Invalid quantity value '%s'; defaulting to 1. Cause: %s", value, e.getMessage());
      return 1;
    }
  }
}
