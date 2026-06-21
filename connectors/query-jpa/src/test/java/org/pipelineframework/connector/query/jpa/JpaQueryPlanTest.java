package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.query.QueryStepDescriptor;

class JpaQueryPlanTest {

    @Test
    void buildsHqlAndBindingsFromDeclarativeWhereMap() {
        JpaQueryPlan plan = JpaQueryPlan.from(descriptor(
            Map.of("customerId", "input.customerId"),
            Map.of()));

        assertEquals("select e from " + CustomerRiskEntity.class.getName() + " e where e.customerId = :p0", plan.toHql());
        assertEquals(Map.of("p0", "customer-1"), plan.bindings(new CustomerRiskLookup("customer-1")));
    }

    @Test
    void rejectsNonInputWhereBindings() {
        QueryStepDescriptor descriptor = descriptor(
            Map.of("customerId", "customerId"),
            Map.of());

        assertThrows(IllegalArgumentException.class, () -> JpaQueryPlan.from(descriptor));
    }

    @Test
    void rejectsUnsafeEntityPropertyNames() {
        QueryStepDescriptor descriptor = descriptor(
            Map.of("customerId = customerId", "input.customerId"),
            Map.of());

        assertThrows(IllegalArgumentException.class, () -> JpaQueryPlan.from(descriptor));
    }

    private static QueryStepDescriptor descriptor(Map<String, String> where, Map<String, String> projection) {
        return new QueryStepDescriptor(
            "LoadCustomerRisk",
            "customer-risk",
            "jpa",
            "v1",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            new PipelineYamlJpaQuery(CustomerRiskEntity.class.getName(), where, projection, "single"));
    }

    record CustomerRiskLookup(String customerId) {
    }

    record CustomerRiskFacts(String customerId, String riskBand) {
    }

    static final class CustomerRiskEntity {
        String customerId;
    }
}
