package org.pipelineframework.examples.quickbooks.domain;

import java.math.BigDecimal;
import java.util.Objects;

/** One customer row from the canonical collections briefing. */
public record CollectionAccount(
    String customer,
    BigDecimal current,
    BigDecimal days1To30,
    BigDecimal days31To60,
    BigDecimal days61To90,
    BigDecimal over90,
    BigDecimal total
) {
    public CollectionAccount {
        Objects.requireNonNull(customer, "customer");
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(days1To30, "days1To30");
        Objects.requireNonNull(days31To60, "days31To60");
        Objects.requireNonNull(days61To90, "days61To90");
        Objects.requireNonNull(over90, "over90");
        Objects.requireNonNull(total, "total");
    }
}
