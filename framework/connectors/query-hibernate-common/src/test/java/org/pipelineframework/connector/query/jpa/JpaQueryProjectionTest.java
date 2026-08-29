package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class JpaQueryProjectionTest {
    @Test
    void returnsTheEntityItselfWhenTheRequestedOutputIsItsExternalType() {
        CustomerRiskEntity entity = new CustomerRiskEntity(
            "customer-1", "HIGH", 97, new Account("ACTIVE"));

        assertSame(entity, JpaQueryProjection.project(entity, CustomerRiskEntity.class, Map.of()));
    }

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

        assertEquals(Optional.of("customer-1"), JpaQueryReflection.readProperty(entity, "customerId"));
        assertThrows(IllegalArgumentException.class, () -> JpaQueryReflection.readProperty(entity, "missing"));
    }

    @Test
    void readsNullFieldValues() {
        NullFieldEntity entity = new NullFieldEntity();

        assertEquals(Optional.empty(), JpaQueryReflection.readProperty(entity, "customerId"));
        assertEquals(new OptionalCustomerRiskFacts(Optional.empty()), JpaQueryProjection.project(
            entity,
            OptionalCustomerRiskFacts.class,
            Map.of()));
        assertThrows(IllegalArgumentException.class, () -> JpaQueryProjection.project(
            entity,
            RequiredCustomerRiskFacts.class,
            Map.of()));
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    record CustomerRiskWithAccount(String customerId, String accountStatus) {
    }

    record OptionalCustomerRiskFacts(Optional<String> customerId) {
    }

    record RequiredCustomerRiskFacts(String customerId) {
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
