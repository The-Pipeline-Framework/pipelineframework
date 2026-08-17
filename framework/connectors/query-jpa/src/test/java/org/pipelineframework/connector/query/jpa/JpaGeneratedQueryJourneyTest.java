package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import org.hibernate.reactive.mutiny.Mutiny;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepDescriptorFactory;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
@QuarkusTestResource(value = JpaGeneratedQueryJourneyTest.PipelineConfiguration.class, restrictToAnnotatedClass = true)
class JpaGeneratedQueryJourneyTest {
    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    ConnectorBindingRegistry bindings;

    @Inject
    QueryStepDescriptorFactory descriptorFactory;

    @Test
    @RunOnVertxContext
    void generatedDescriptorReachesInjectedProviderAndProjectsTheDeclaredOutputType(UniAsserter asserter) {
        Uni<QueryStepDescriptor> descriptor = descriptorFactory.descriptor(
            "LoadCustomerRisk",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName());
        GeneratedJpaQueryStep step = new GeneratedJpaQueryStep(
            new QueryStepSupport(List.of(), List.of(), bindings), descriptor);

        asserter.assertThat(
            () -> sessionFactory.withTransaction((session, tx) ->
                session.createMutationQuery("delete from " + CustomerRiskEntity.class.getName()).executeUpdate()
                    .replaceWithVoid()
                    .chain(() -> session.persist(new CustomerRiskEntity("customer-generated", "HIGH", 97))))
                .chain(() -> step.applyOneToOne(new CustomerRiskLookup("customer-generated"))),
            output -> assertEquals(new CustomerRiskFacts("customer-generated", "HIGH", 97), output));
    }

    public static final class PipelineConfiguration implements QuarkusTestResourceLifecycleManager {
        private String previous;
        private String previousValidation;

        @Override
        public Map<String, String> start() {
            previous = System.getProperty("pipeline.config");
            previousValidation = System.getProperty("smallrye.config.mapping.validate-unknown");
            System.setProperty("smallrye.config.mapping.validate-unknown", "false");
            System.setProperty(
                "pipeline.config",
                Path.of("src/test/resources/jpa-provider-query-pipeline.yaml").toAbsolutePath().toString());
            return Map.of();
        }

        @Override
        public void stop() {
            if (previous == null) {
                System.clearProperty("pipeline.config");
            } else {
                System.setProperty("pipeline.config", previous);
            }
            if (previousValidation == null) {
                System.clearProperty("smallrye.config.mapping.validate-unknown");
            } else {
                System.setProperty("smallrye.config.mapping.validate-unknown", previousValidation);
            }
        }
    }

    record CustomerRiskLookup(String customerId) {
    }

    record CustomerRiskFacts(String customerId, String riskBand, int score) {
    }

    private static final class GeneratedJpaQueryStep extends ConfigurableStep
        implements StepOneToOne<CustomerRiskLookup, CustomerRiskFacts> {
        private final QueryStepSupport support;
        private final Uni<QueryStepDescriptor> descriptor;

        private GeneratedJpaQueryStep(QueryStepSupport support, Uni<QueryStepDescriptor> descriptor) {
            this.support = support;
            this.descriptor = descriptor;
        }

        @Override
        public Uni<CustomerRiskFacts> applyOneToOne(CustomerRiskLookup input) {
            return support.queryOneToOne(descriptor, input, CustomerRiskFacts.class);
        }
    }
}
