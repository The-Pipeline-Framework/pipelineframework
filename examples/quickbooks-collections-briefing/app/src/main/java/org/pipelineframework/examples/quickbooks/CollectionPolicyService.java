package org.pipelineframework.examples.quickbooks;

import java.math.BigDecimal;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.examples.quickbooks.domain.CollectionAccount;
import org.pipelineframework.examples.quickbooks.domain.CollectionAction;
import org.pipelineframework.service.ReactiveService;

/** Pure, deterministic collections policy applied independently to each customer account. */
@ApplicationScoped
public final class CollectionPolicyService implements ReactiveService<CollectionAccount, CollectionAction> {
    private static final BigDecimal CRITICAL_OVER_90 = new BigDecimal("10000");
    private static final BigDecimal HIGH_OVERDUE = new BigDecimal("1000");

    @Override
    public Uni<CollectionAction> process(CollectionAccount account) {
        return Uni.createFrom().item(() -> assess(account));
    }

    static CollectionAction assess(CollectionAccount account) {
        if (!account.accountPresent()) {
            return new CollectionAction(account, BigDecimal.ZERO, "NONE",
                "The report contains no open customer receivables.",
                "No collection action is required.");
        }
        BigDecimal overdue = account.days1To30()
            .add(account.days31To60())
            .add(account.days61To90())
            .add(account.over90());
        if (account.over90().compareTo(CRITICAL_OVER_90) >= 0) {
            return new CollectionAction(account, overdue, "CRITICAL",
                "At least 10,000 has aged beyond 90 days.",
                "Call today, confirm the balance details, and agree a dated payment plan.");
        }
        if (overdue.compareTo(HIGH_OVERDUE) >= 0) {
            return new CollectionAction(account, overdue, "HIGH",
                "At least 1,000 is overdue.",
                "Contact within one business day and request a firm payment date.");
        }
        if (overdue.signum() > 0) {
            return new CollectionAction(account, overdue, "STANDARD",
                "The account has an overdue balance below the escalation threshold.",
                "Send a personalised reminder and review again in seven days.");
        }
        return new CollectionAction(account, overdue, "MONITOR",
            "The open balance is not yet overdue.",
            "No collection contact; monitor the next aging report.");
    }
}
