package org.pipelineframework.checkout.createorder.order_create.service;

import org.pipelineframework.checkout.createorder.common.domain.OrderLineItem;
import org.pipelineframework.checkout.createorder.common.domain.InitialOrder;
import org.pipelineframework.annotation.PipelineStep;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = OrderLineItem.class,
    outputType = InitialOrder.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = org.pipelineframework.checkout.createorder.common.mapper.OrderLineItemMapper.class,
    outboundMapper = org.pipelineframework.checkout.createorder.common.mapper.InitialOrderMapper.class
)
@ApplicationScoped
public class ProcessOrderCreateService
    implements ReactiveService<OrderLineItem, InitialOrder> {

  /**
   * Builds an InitialOrder using fields from the given OrderLineItem and a deterministic UUID derived from those fields.
   *
   * <p>Validates that the input and its required fields (requestId, customerId, sku) are present and sku is not blank;
   * if validation fails the method returns a failed Uni with an IllegalArgumentException.</p>
   *
   * @param input the order line item to process; must have non-null requestId and customerId, and a non-blank sku
   * @return an InitialOrder containing a deterministic orderId (derived from requestId, customerId, sku, and quantity),
   *         the input's customerId, and the normalized quantity
   */
  @Override
  public Uni<InitialOrder> process(OrderLineItem input) {
    if (input == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("lineItem must not be null"));
    }
    if (input.requestId() == null || input.customerId() == null || input.sku() == null || input.sku().isBlank()) {
      return Uni.createFrom().failure(new IllegalArgumentException(
          "lineItem.requestId, lineItem.customerId and lineItem.sku must not be null/blank"));
    }
    int normalizedQuantity = input.quantity();
    String trimmedSku = input.sku().trim();
    String requestPart = input.requestId().toString();
    String customerPart = input.customerId().toString();
    String quantityPart = Integer.toString(normalizedQuantity);
    String seed = requestPart.length() + ":" + requestPart
        + "|" + customerPart.length() + ":" + customerPart
        + "|" + trimmedSku.length() + ":" + trimmedSku
        + "|" + quantityPart.length() + ":" + quantityPart;
    UUID orderId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    InitialOrder output = new InitialOrder(orderId, input.customerId(), normalizedQuantity);
    return Uni.createFrom().item(output);
  }
}