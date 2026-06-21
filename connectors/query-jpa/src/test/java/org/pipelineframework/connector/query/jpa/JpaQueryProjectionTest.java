package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

class JpaQueryProjectionTest {

    @Test
    void projectsEntityPropertiesIntoRecordOutput() {
        CustomerRiskEntity entity = new CustomerRiskEntity("customer-1", "HIGH", 83);

        CustomerRiskFacts facts = JpaQueryProjection.project(
            entity,
            CustomerRiskFacts.class,
            Map.of("score", "riskScore"));

        assertEquals(new CustomerRiskFacts("customer-1", "HIGH", 83), facts);
    }

    @Test
    void rejectsNonRecordOutputs() {
        CustomerRiskEntity entity = new CustomerRiskEntity("customer-1", "HIGH", 83);

        assertThrows(IllegalArgumentException.class, () ->
            JpaQueryProjection.project(entity, CustomerRiskBean.class, Map.of()));
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    static final class CustomerRiskBean {
    }

    static final class CustomerRiskEntity {
        private final String customerId;
        private final String riskBand;
        private final int riskScore;

        CustomerRiskEntity(String customerId, String riskBand, int riskScore) {
            this.customerId = customerId;
            this.riskBand = riskBand;
            this.riskScore = riskScore;
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
    }
}
