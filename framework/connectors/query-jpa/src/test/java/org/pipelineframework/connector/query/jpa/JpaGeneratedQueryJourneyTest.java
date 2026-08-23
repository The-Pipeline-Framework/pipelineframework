package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepDescriptorFactory;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.proto.PayloadReferenceProtobufCodec;
import org.pipelineframework.repository.PayloadReference;

@QuarkusTest
@QuarkusTestResource(value = JpaGeneratedQueryJourneyTest.PipelineConfiguration.class, restrictToAnnotatedClass = true)
class JpaGeneratedQueryJourneyTest {
    @Inject
    EntityManagerFactory entityManagerFactory;

    @Inject
    ConnectorBindingRegistry bindings;

    @Inject
    QueryStepDescriptorFactory descriptorFactory;

    @Test
    void generatedDescriptorReachesInjectedProviderAndProjectsTheDeclaredOutputType() {
        Uni<QueryStepDescriptor> descriptor = descriptorFactory.descriptor(
            "LoadCustomerRisk",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName());
        GeneratedJpaQueryStep step = new GeneratedJpaQueryStep(
            new QueryStepSupport(List.of(), List.of(), bindings), descriptor);

        replaceWith(CustomerRiskEntity.class, new CustomerRiskEntity("customer-generated", "HIGH", 97));

        CustomerRiskFacts output = step.applyOneToOne(
            new CustomerRiskLookup("customer-generated")).await().indefinitely();

        assertEquals(new CustomerRiskFacts("customer-generated", "HIGH", 97), output);
    }

    @Test
    void persistenceMapperRoundTripsOpaquePayloadReferencesThroughFindOne() {
        Thread callerThread = Thread.currentThread();
        InvoiceFilesEntity.LAST_LOAD_THREAD.set(null);
        UUID documentId = UUID.randomUUID();
        InvoiceFiles expected = new InvoiceFiles(
            documentId,
            "invoice.pdf",
            reference("invoice.pdf", "invoice-sha"),
            reference("config.yaml", "catalogue-sha"));
        InvoiceFilesPersistenceMapper mapper = new InvoiceFilesPersistenceMapper();
        Uni<QueryStepDescriptor> descriptor = descriptorFactory.descriptor(
            "LoadInvoiceFiles",
            InvoiceLookup.class.getName(),
            InvoiceFiles.class.getName());

        replaceWith(InvoiceFilesEntity.class, mapper.toExternal(expected));

        InvoiceFiles actual = new QueryStepSupport(List.of(), List.of(), bindings)
            .queryOneToOne(
                descriptor,
                new InvoiceLookup(documentId),
                InvoiceFiles.class,
                InvoiceFilesEntity.class,
                mapper)
            .await().indefinitely();

        assertEquals(expected, actual);
        Thread loadThread = InvoiceFilesEntity.LAST_LOAD_THREAD.get();
        assertNotEquals(callerThread, loadThread);
        assertTrue(loadThread.isVirtual(), "JPA reads must run on the connector's virtual-thread executor");
    }

    private void replaceWith(Class<?> entityType, Object entity) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        var transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            entityManager.createQuery("delete from " + entityType.getName()).executeUpdate();
            entityManager.persist(entity);
            transaction.commit();
        } finally {
            if (transaction.isActive()) {
                transaction.rollback();
            }
            entityManager.close();
        }
    }

    private static PayloadReference reference(String key, String checksum) {
        return new PayloadReference(
            "repository", "invoice-work", key, "application/octet-stream", "raw", checksum, 42,
            "v1", Map.of("locator", key), Optional.empty());
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

    record InvoiceLookup(UUID documentId) {
    }

    record InvoiceFiles(
        UUID documentId,
        String originalFilename,
        PayloadReference invoice,
        PayloadReference catalogue
    ) {
    }

    static final class InvoiceFilesPersistenceMapper implements Mapper<InvoiceFiles, InvoiceFilesEntity> {
        @Override
        public InvoiceFiles fromExternal(InvoiceFilesEntity external) {
            assertEquals(List.of("canonical-payloads"), external.evidenceLabels);
            return new InvoiceFiles(
                external.documentId,
                external.originalFilename,
                PayloadReferenceProtobufCodec.decode(Base64.getDecoder().decode(external.invoiceReference)),
                PayloadReferenceProtobufCodec.decode(Base64.getDecoder().decode(external.catalogueReference)));
        }

        @Override
        public InvoiceFilesEntity toExternal(InvoiceFiles domain) {
            var entity = new InvoiceFilesEntity();
            entity.documentId = domain.documentId();
            entity.originalFilename = domain.originalFilename();
            entity.invoiceReference = Base64.getEncoder().encodeToString(
                PayloadReferenceProtobufCodec.encode(domain.invoice()));
            entity.catalogueReference = Base64.getEncoder().encodeToString(
                PayloadReferenceProtobufCodec.encode(domain.catalogue()));
            entity.evidenceLabels.add("canonical-payloads");
            return entity;
        }
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
