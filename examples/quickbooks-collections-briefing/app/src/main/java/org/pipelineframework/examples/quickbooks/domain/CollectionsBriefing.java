package org.pipelineframework.examples.quickbooks.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/** Typed business result produced from the captured QuickBooks report. */
public record CollectionsBriefing(
    String reportDate,
    String currency,
    BigDecimal totalOutstanding,
    BigDecimal overdueOutstanding,
    long priorityAccounts,
    String headline,
    List<CollectionAction> actions
) {
    public CollectionsBriefing {
        Objects.requireNonNull(reportDate, "reportDate");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(totalOutstanding, "totalOutstanding");
        Objects.requireNonNull(overdueOutstanding, "overdueOutstanding");
        Objects.requireNonNull(headline, "headline");
        actions = List.copyOf(actions);
    }
}
