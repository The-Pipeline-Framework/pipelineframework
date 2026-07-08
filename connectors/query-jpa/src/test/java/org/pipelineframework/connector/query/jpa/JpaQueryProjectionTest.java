package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

class JpaQueryProjectionTest {

    @Test
    void projectsEntityPropertiesIntoRecordOutput() {
        CustomerRiskEntity entity = new CustomerRiskEntity("customer-1", "HIGH", 83, new Account("ACTIVE"));

        CustomerRiskFacts facts = JpaQueryProjection.project(
            entity,
            CustomerRiskFacts.class,
            Map.of("score", "riskScore"));

        assertEquals(new CustomerRiskFacts("customer-1", "HIGH", 83), facts);
    }

    @Test
    void projectsDottedEntityPropertiesIntoRecordOutput() {
        CustomerRiskEntity entity = new CustomerRiskEntity("customer-1", "HIGH", 83, new Account("ACTIVE"));

        CustomerRiskWithAccount facts = JpaQueryProjection.project(
            entity,
            CustomerRiskWithAccount.class,
            Map.of("accountStatus", "account.status"));

        assertEquals(new CustomerRiskWithAccount("customer-1", "ACTIVE"), facts);
    }

    @Test
    void rejectsNonRecordOutputs() {
        CustomerRiskEntity entity = new CustomerRiskEntity("customer-1", "HIGH", 83, new Account("ACTIVE"));

        assertThrows(IllegalArgumentException.class, () ->
            JpaQueryProjection.project(entity, CustomerRiskBean.class, Map.of()));
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    record CustomerRiskWithAccount(String customerId, String accountStatus) {
    }

    record Account(String status) {
    }

    static final class CustomerRiskBean {
    }

    static final class CustomerRiskEntity {
        private final String customerId;
        private final String riskBand;
        private final int riskScore;
        private final Account account;

        CustomerRiskEntity(String customerId, String riskBand, int riskScore, Account account) {
            this.customerId = customerId;
            this.riskBand = riskBand;
            this.riskScore = riskScore;
            this.account = account;
        }

        public String getCustomerId() {
            return customerId;
        }

        public String riskBand() {
            return riskBand;
        }

        public int getRiskScore() {
            return riskScore;
        }

        public Account account() {
            return account;
        }
    }
}
