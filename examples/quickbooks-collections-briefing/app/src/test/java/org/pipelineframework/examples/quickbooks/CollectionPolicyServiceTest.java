package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import org.pipelineframework.examples.quickbooks.domain.CollectionAccount;

class CollectionPolicyServiceTest {
    @Test
    void assignsDeterministicInterventionLanes() {
        assertEquals("CRITICAL", CollectionPolicyService.assess(account("12000", "12000")).priority());
        assertEquals("HIGH", CollectionPolicyService.assess(account("0", "1800")).priority());
        assertEquals("STANDARD", CollectionPolicyService.assess(account("0", "480")).priority());
        assertEquals("MONITOR", CollectionPolicyService.assess(account("0", "0")).priority());
    }

    private static CollectionAccount account(String over90, String total) {
        BigDecimal over90Amount = new BigDecimal(over90);
        BigDecimal totalAmount = new BigDecimal(total);
        return new CollectionAccount("2026-09-08", "GBP", true, "1", "Customer", BigDecimal.ZERO,
            totalAmount.subtract(over90Amount), BigDecimal.ZERO, BigDecimal.ZERO, over90Amount, totalAmount);
    }
}
