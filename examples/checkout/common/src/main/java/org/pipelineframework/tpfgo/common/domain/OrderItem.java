package org.pipelineframework.tpfgo.common.domain;

public record OrderItem(
    String sku,
    int quantity
) {
    public OrderItem {
        sku = CommonDomainValidation.requireNonBlank(sku, "sku");
        quantity = CommonDomainValidation.requirePositive(quantity, "quantity");
    }
}

