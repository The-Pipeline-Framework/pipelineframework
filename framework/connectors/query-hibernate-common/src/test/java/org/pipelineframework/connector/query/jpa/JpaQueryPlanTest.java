package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class JpaQueryPlanTest {

    @Test
    void buildsHqlAndBindingsFromDeclarativeWhereMap() {
        JpaQueryPlan plan = plan(descriptor(
            Map.of("customerId", "input.customerId"),
            Map.of()));

        assertEquals("select e from " + CustomerRiskEntity.class.getName() + " e where e.customerId = :p0", plan.toHql());
        assertEquals(Map.of("p0", "customer-1"), plan.bindings(new CustomerRiskLookup("customer-1", 80, List.of(), new String[0])));
    }

    @Test
    void buildsHqlAndBindingsForSupportedPredicates() {
        Map<String, JpaPredicate> where = new LinkedHashMap<>();
        where.put("customerId", new JpaPredicate("eq", List.of("input.customerId")));
        where.put("score", new JpaPredicate("gte", List.of("input.minimumScore")));
        where.put("riskBand", new JpaPredicate("in", List.of("HIGH", "CRITICAL")));
        where.put("status", new JpaPredicate("like", List.of("ACT%")));
        where.put("deletedAt", new JpaPredicate("isNull", List.of(true)));
        where.put("account.status", new JpaPredicate("isNull", List.of(false)));

        JpaQueryPlan plan = plan(configuration(where, Map.of(), Map.of(), null));

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
        Map<String, JpaPredicate> collectionWhere = new LinkedHashMap<>();
        collectionWhere.put("riskBand", new JpaPredicate("in", List.of("input.riskBands")));
        JpaQueryPlan collectionPlan = plan(configuration(collectionWhere, Map.of(), Map.of(), null));

        Map<String, JpaPredicate> arrayWhere = new LinkedHashMap<>();
        arrayWhere.put("status", new JpaPredicate("in", List.of("input.statuses")));
        JpaQueryPlan arrayPlan = plan(configuration(arrayWhere, Map.of(), Map.of(), null));

        CustomerRiskLookup input = new CustomerRiskLookup("customer-1", 80, List.of("HIGH", "CRITICAL"), new String[] {"ACTIVE", "PENDING"});
        assertEquals(Map.of("p0", List.of("HIGH", "CRITICAL")), collectionPlan.bindings(input));
        assertEquals(Map.of("p0", List.of("ACTIVE", "PENDING")), arrayPlan.bindings(input));
    }

    @Test
    void rendersBetweenPredicateWithTwoBindings() {
        Map<String, JpaPredicate> where = new LinkedHashMap<>();
        where.put("score", new JpaPredicate("between", List.of(50, "input.minimumScore")));
        JpaQueryPlan plan = plan(configuration(where, Map.of(), Map.of(), null));

        assertEquals(
            "select e from " + CustomerRiskEntity.class.getName() + " e where e.score between :p0 and :p1",
            plan.toHql());
        assertEquals(Map.of("p0", 50, "p1", 80), plan.bindings(new CustomerRiskLookup("customer-1", 80, List.of(), new String[0])));
    }

    @Test
    void rendersOrderByAndLimit() {
        Map<String, JpaPredicate> where = new LinkedHashMap<>();
        where.put("customerId", new JpaPredicate("eq", List.of("input.customerId")));
        Map<String, String> orderBy = new LinkedHashMap<>();
        orderBy.put("updatedAt", "desc");
        orderBy.put("account.status", "asc");

        JpaQueryPlan plan = plan(configuration(where, Map.of(), orderBy, 1));

        assertEquals(
            "select e from " + CustomerRiskEntity.class.getName()
                + " e where e.customerId = :p0 order by e.updatedAt desc, e.account.status asc",
            plan.toHql());
        assertEquals(1, plan.maxResults());
        assertEquals(true, plan.firstResultOnly());
    }

    @Test
    void rejectsUnsafeEntityPropertyNames() {
        JpaFindOneConfiguration configuration = descriptor(
            Map.of("customerId = customerId", "input.customerId"),
            Map.of());

        assertThrows(IllegalArgumentException.class, () -> plan(configuration));
    }

    @Test
    void rejectsUnsafeDottedPaths() {
        Map<String, JpaPredicate> where = new LinkedHashMap<>();
        where.put("account..status", new JpaPredicate("eq", List.of("ACTIVE")));

        assertThrows(IllegalArgumentException.class, () -> plan(configuration(where, Map.of(), Map.of(), null)));
    }

    private static JpaFindOneConfiguration descriptor(Map<String, String> where, Map<String, String> projection) {
        Map<String, JpaPredicate> predicates = new LinkedHashMap<>();
        where.forEach((path, value) -> predicates.put(path, new JpaPredicate("eq", List.of(value))));
        return configuration(predicates, projection, Map.of(), null);
    }

    private static JpaFindOneConfiguration configuration(
        Map<String, JpaPredicate> where,
        Map<String, String> projection,
        Map<String, String> orderBy,
        Integer limit
    ) {
        return new JpaFindOneConfiguration(
            CustomerRiskEntity.class.getName(), where, Optional.of(projection), Optional.of(orderBy),
            Optional.ofNullable(limit), Optional.of("single"));
    }

    private static JpaQueryPlan plan(JpaFindOneConfiguration configuration) {
        return JpaQueryPlan.from("customer-risk", configuration);
    }

    record CustomerRiskLookup(String customerId, int minimumScore, List<String> riskBands, String[] statuses) {
    }

    record CustomerRiskFacts(String customerId, String riskBand) {
    }

    static final class CustomerRiskEntity {
        String customerId;
    }
}
