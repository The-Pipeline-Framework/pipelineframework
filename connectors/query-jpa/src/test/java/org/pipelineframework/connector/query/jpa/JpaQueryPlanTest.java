package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.config.pipeline.PipelineYamlJpaPredicate;
import org.pipelineframework.query.QueryStepDescriptor;

class JpaQueryPlanTest {

    @Test
    void buildsHqlAndBindingsFromDeclarativeWhereMap() {
        JpaQueryPlan plan = JpaQueryPlan.from(descriptor(
            Map.of("customerId", "input.customerId"),
            Map.of()));

        assertEquals("select e from " + CustomerRiskEntity.class.getName() + " e where e.customerId = :p0", plan.toHql());
        assertEquals(Map.of("p0", "customer-1"), plan.bindings(new CustomerRiskLookup("customer-1", 80, List.of(), new String[0])));
    }

    @Test
    void buildsHqlAndBindingsForSupportedPredicates() {
        Map<String, PipelineYamlJpaPredicate> where = new LinkedHashMap<>();
        where.put("customerId", PipelineYamlJpaPredicate.equalTo("input.customerId"));
        where.put("score", new PipelineYamlJpaPredicate("gte", List.of("input.minimumScore")));
        where.put("riskBand", new PipelineYamlJpaPredicate("in", List.of("HIGH", "CRITICAL")));
        where.put("status", new PipelineYamlJpaPredicate("like", List.of("ACT%")));
        where.put("deletedAt", new PipelineYamlJpaPredicate("isNull", List.of(true)));
        where.put("account.status", new PipelineYamlJpaPredicate("isNull", List.of(false)));

        JpaQueryPlan plan = JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(),
            where,
            Map.of(),
            Map.of(),
            null,
            "single")));

        assertEquals(
            "select e from " + CustomerRiskEntity.class.getName()
                + " e where e.customerId = :p0 and e.score >= :p1 and e.riskBand in :p2"
                + " and e.status like :p3 and e.deletedAt is null and e.account.status is not null",
            plan.toHql());
        assertEquals(
            Map.of("p0", "customer-1", "p1", 80, "p2", List.of("HIGH", "CRITICAL"), "p3", "ACT%"),
            plan.bindings(new CustomerRiskLookup("customer-1", 80, List.of(), new String[0])));
    }

    @Test
    void bindsInPredicateFromInputCollectionAndArray() {
        Map<String, PipelineYamlJpaPredicate> collectionWhere = new LinkedHashMap<>();
        collectionWhere.put("riskBand", new PipelineYamlJpaPredicate("in", List.of("input.riskBands")));
        JpaQueryPlan collectionPlan = JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(), collectionWhere, Map.of(), Map.of(), null, "single")));

        Map<String, PipelineYamlJpaPredicate> arrayWhere = new LinkedHashMap<>();
        arrayWhere.put("status", new PipelineYamlJpaPredicate("in", List.of("input.statuses")));
        JpaQueryPlan arrayPlan = JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(), arrayWhere, Map.of(), Map.of(), null, "single")));

        CustomerRiskLookup input = new CustomerRiskLookup("customer-1", 80, List.of("HIGH", "CRITICAL"), new String[] {"ACTIVE", "PENDING"});
        assertEquals(Map.of("p0", List.of("HIGH", "CRITICAL")), collectionPlan.bindings(input));
        assertEquals(Map.of("p0", List.of("ACTIVE", "PENDING")), arrayPlan.bindings(input));
    }

    @Test
    void rendersBetweenPredicateWithTwoBindings() {
        Map<String, PipelineYamlJpaPredicate> where = new LinkedHashMap<>();
        where.put("score", new PipelineYamlJpaPredicate("between", List.of(50, "input.minimumScore")));
        JpaQueryPlan plan = JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(), where, Map.of(), Map.of(), null, "single")));

        assertEquals(
            "select e from " + CustomerRiskEntity.class.getName() + " e where e.score between :p0 and :p1",
            plan.toHql());
        assertEquals(Map.of("p0", 50, "p1", 80), plan.bindings(new CustomerRiskLookup("customer-1", 80, List.of(), new String[0])));
    }

    @Test
    void rendersOrderByAndLimit() {
        Map<String, PipelineYamlJpaPredicate> where = new LinkedHashMap<>();
        where.put("customerId", PipelineYamlJpaPredicate.equalTo("input.customerId"));
        Map<String, String> orderBy = new LinkedHashMap<>();
        orderBy.put("updatedAt", "desc");
        orderBy.put("account.status", "asc");

        JpaQueryPlan plan = JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(), where, Map.of(), orderBy, 1, "single")));

        assertEquals(
            "select e from " + CustomerRiskEntity.class.getName()
                + " e where e.customerId = :p0 order by e.updatedAt desc, e.account.status asc",
            plan.toHql());
        assertEquals(1, plan.maxResults());
        assertEquals(true, plan.firstResultOnly());
    }

    @Test
    void rejectsUnsafeEntityPropertyNames() {
        QueryStepDescriptor descriptor = descriptor(
            Map.of("customerId = customerId", "input.customerId"),
            Map.of());

        assertThrows(IllegalArgumentException.class, () -> JpaQueryPlan.from(descriptor));
    }

    @Test
    void rejectsUnsafeDottedPaths() {
        Map<String, PipelineYamlJpaPredicate> where = new LinkedHashMap<>();
        where.put("account..status", PipelineYamlJpaPredicate.equalTo("ACTIVE"));

        assertThrows(IllegalArgumentException.class, () -> JpaQueryPlan.from(descriptor(new PipelineYamlJpaQuery(
            CustomerRiskEntity.class.getName(), where, Map.of(), Map.of(), null, "single"))));
    }

    private static QueryStepDescriptor descriptor(Map<String, String> where, Map<String, String> projection) {
        return descriptor(new PipelineYamlJpaQuery(CustomerRiskEntity.class.getName(), where, projection, "single"));
    }

    private static QueryStepDescriptor descriptor(PipelineYamlJpaQuery jpa) {
        return new QueryStepDescriptor(
            "LoadCustomerRisk",
            "customer-risk",
            "jpa",
            "v1",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName(),
            "ONE_TO_ONE",
            List.of("customerId"),
            jpa);
    }

    record CustomerRiskLookup(String customerId, int minimumScore, List<String> riskBands, String[] statuses) {
    }

    record CustomerRiskFacts(String customerId, String riskBand) {
    }

    static final class CustomerRiskEntity {
        String customerId;
    }
}
