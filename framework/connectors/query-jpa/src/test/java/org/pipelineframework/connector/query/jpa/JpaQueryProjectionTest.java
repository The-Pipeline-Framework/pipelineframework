package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    void readsInheritedFieldsAndRejectsMissingProperties() {
        InheritedFieldEntity entity = new InheritedFieldEntity("customer-1");

        assertEquals("customer-1", JpaQueryReflection.readProperty(entity, "customerId"));
        assertThrows(IllegalArgumentException.class, () -> JpaQueryReflection.readProperty(entity, "missing"));
    }

    @Test
    void readsNullFieldValues() {
        assertNull(JpaQueryReflection.readProperty(new NullFieldEntity(), "customerId"));
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    record CustomerRiskWithAccount(String customerId, String accountStatus) {
    }

    record Account(String status) {
    }

    static final class CustomerRiskBean {
    }

    static class BaseEntity {
        private final String customerId;

        BaseEntity(String customerId) {
            this.customerId = customerId;
        }
    }

    static final class InheritedFieldEntity extends BaseEntity {
        InheritedFieldEntity(String customerId) {
            super(customerId);
        }
    }

    static final class NullFieldEntity {
        private final String customerId = null;
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
