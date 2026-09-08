package org.pipelineframework.examples.quickbooks.domain;

import java.math.BigDecimal;
import java.util.Objects;

/** A deterministic intervention recommendation for one receivables account. */
public record CollectionAction(
    CollectionAccount account,
    BigDecimal overdue,
    String priority,
    String rationale,
    String recommendedAction
) {
    public CollectionAction {
        Objects.requireNonNull(account, "account");
        Objects.requireNonNull(overdue, "overdue");
        Objects.requireNonNull(priority, "priority");
        Objects.requireNonNull(rationale, "rationale");
        Objects.requireNonNull(recommendedAction, "recommendedAction");
    }
}
