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
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class
)
@ApplicationScoped
public class ProcessOrderRequestProcessService
    implements ReactiveService<OrderRequest, OrderLineItem> {

  private static final Logger LOG = Logger.getLogger(ProcessOrderRequestProcessService.class);

  /**
   * Processes an OrderRequest and produces an OrderLineItem based on the first comma-separated item.
   *
   * The created OrderLineItem uses the requestId and customerId from the input, the SKU parsed from the
   * first item token (defaults to "unknown-sku" if blank), and a parsed quantity (defaults to 1 for null,
   * non-numeric, or non-positive values).
   *
   * @param input the incoming order request (the first item of input.items() is used); may be null
   * @return the resulting OrderLineItem containing requestId, customerId, resolved SKU, and quantity
   */
  @Override
  public Uni<OrderLineItem> process(OrderRequest input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("input must not be null"));
    }
    String raw = input.items() == null ? "" : input.items().trim();
    String[] tokens = raw.isEmpty() ? new String[0] : raw.split(",");
    String first = tokens.length == 0 ? "" : tokens[0];
    if (tokens.length > 1) {
      LOG.debugf("Multiple line items received '%s'; processing first token '%s'", raw, first);
    }
    String[] parts = first.trim().split(":");
    String sku = !parts[0].isBlank() ? parts[0].trim() : "unknown-sku";
    int quantity = parts.length > 1 ? parseQuantity(parts[1]) : 1;
    OrderLineItem output = new OrderLineItem(input.requestId(), input.customerId(), sku, quantity);
    return Uni.createFrom().item(output);
  }

  /**
   * Parse a quantity string into a positive integer, defaulting to 1 for null, non-numeric, or non-positive inputs.
   *
   * @param value the quantity string to parse; may be null, blank, or malformed
   * @return the parsed integer if greater than zero, otherwise 1
   */
  private static int parseQuantity(String value) {
    if (value == null) {
      LOG.warn("Invalid quantity value <null>; defaulting to 1");
      return 1;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed <= 0) {
        LOG.warnf("Non-positive quantity value '%s' parsed as %s; defaulting to 1", value, parsed);
        return 1;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOG.warnf("Invalid quantity value '%s'; defaulting to 1. Cause: %s", value, e.getMessage());
      return 1;
    }
  }
}