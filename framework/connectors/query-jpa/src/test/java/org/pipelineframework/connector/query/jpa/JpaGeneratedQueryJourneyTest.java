package org.pipelineframework.connector.query.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepDescriptorFactory;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.StepOneToMany;
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

    @AfterEach
    void clearExecutionContext() {
        PipelineExecutionContextHolder.clear();
    }

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
    }

    @Test
    void generatedOneToManyStepStreamsRowsFromTheBlockingProvider() {
        Uni<QueryStepDescriptor> descriptor = descriptorFactory.descriptor(
            "LoadCustomerRisks",
            CustomerRiskLookup.class.getName(),
            CustomerRiskFacts.class.getName());
        GeneratedJpaStreamingQueryStep step = new GeneratedJpaStreamingQueryStep(
            new QueryStepSupport(List.of(), List.of(), bindings), descriptor);
        replaceWith(
            CustomerRiskEntity.class,
            new CustomerRiskEntity("customer-many", "HIGH", 97),
            new CustomerRiskEntity("customer-many", "LOW", 40),
            new CustomerRiskEntity("customer-many", "MEDIUM", 72));

        List<CustomerRiskFacts> output = step.applyOneToMany(new CustomerRiskLookup("customer-many"))
            .collect().asList().await().indefinitely();

        assertEquals(List.of(
            new CustomerRiskFacts("customer-many", "LOW", 40),
            new CustomerRiskFacts("customer-many", "MEDIUM", 72),
            new CustomerRiskFacts("customer-many", "HIGH", 97)), output);
    }

    @Test
    void streamingPersistenceMapperCanonicalizesEveryRowBeforeAdmission() {
        UUID firstId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID secondId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        StreamedInvoiceFilesPersistenceMapper mapper = new StreamedInvoiceFilesPersistenceMapper();
        InvoiceFiles first = new InvoiceFiles(
            firstId, "batch.pdf", reference("invoice-1.pdf", "sha-1"), reference("catalogue-1", "cat-1"));
        InvoiceFiles second = new InvoiceFiles(
            secondId, "batch.pdf", reference("invoice-2.pdf", "sha-2"), reference("catalogue-2", "cat-2"));
        Uni<QueryStepDescriptor> descriptor = descriptorFactory.descriptor(
            "LoadInvoiceFilesBatch",
            InvoiceBatchLookup.class.getName(),
            InvoiceFiles.class.getName());
        replaceWith(StreamedInvoiceFilesEntity.class, mapper.toExternal(first), mapper.toExternal(second));

        List<InvoiceFiles> output = new QueryStepSupport(List.of(), List.of(), bindings)
            .queryOneToMany(
                descriptor,
                new InvoiceBatchLookup("batch.pdf"),
                InvoiceFiles.class,
                StreamedInvoiceFilesEntity.class,
                mapper)
            .collect().asList().await().indefinitely();

        assertEquals(List.of(first, second), output);
    }

    @Test
    void streamingCaptureReplaysCanonicalRowsWithoutAnotherDatabaseQuery() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "jpa-capture", 2));
        InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
        QueryStepSupport support = new QueryStepSupport(List.of(), List.of(captureStore), bindings);
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadCustomerRisks", CustomerRiskLookup.class.getName(), CustomerRiskFacts.class.getName())
            .await().indefinitely();
        replaceWith(
            CustomerRiskEntity.class,
            new CustomerRiskEntity("captured", "HIGH", 97),
            new CustomerRiskEntity("captured", "LOW", 40));

        List<CustomerRiskFacts> first = support.queryOneToMany(
            descriptor, new CustomerRiskLookup("captured"), CustomerRiskFacts.class)
            .collect().asList().await().indefinitely();
        replaceWith(CustomerRiskEntity.class, new CustomerRiskEntity("captured", "CHANGED", 1));
        List<CustomerRiskFacts> replay = support.queryOneToMany(
            descriptor, new CustomerRiskLookup("captured"), CustomerRiskFacts.class)
            .collect().asList().await().indefinitely();

        assertEquals(first, replay);
        assertEquals(List.of(
            new CustomerRiskFacts("captured", "LOW", 40),
            new CustomerRiskFacts("captured", "HIGH", 97)), replay);
    }

    @Test
    void partialMappedFailureAbortsCaptureAndRetryReevaluatesTheDatabase() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "jpa-partial", 3));
        InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
        QueryStepSupport support = new QueryStepSupport(List.of(), List.of(captureStore), bindings);
        QueryStepDescriptor descriptor = descriptorFactory.descriptor(
            "LoadInvoiceFilesBatch", InvoiceBatchLookup.class.getName(), InvoiceFiles.class.getName())
            .await().indefinitely();
        StreamedInvoiceFilesPersistenceMapper mapper = new StreamedInvoiceFilesPersistenceMapper();
        StreamedInvoiceFilesPersistenceMapper.armPartialFailure();
        InvoiceFiles first = invoice("00000000-0000-0000-0000-000000000001", "batch-failure.pdf", "one");
        InvoiceFiles second = invoice("00000000-0000-0000-0000-000000000002", "batch-failure.pdf", "two");
        replaceWith(StreamedInvoiceFilesEntity.class, mapper.toExternal(first), mapper.toExternal(second));

        assertThrows(IllegalStateException.class, () -> support.queryOneToMany(
                descriptor, new InvoiceBatchLookup("batch-failure.pdf"), InvoiceFiles.class,
                StreamedInvoiceFilesEntity.class, mapper)
            .collect().asList().await().indefinitely());

        InvoiceFiles third = invoice("00000000-0000-0000-0000-000000000003", "batch-failure.pdf", "three");
        replaceWith(
            StreamedInvoiceFilesEntity.class,
            mapper.toExternal(first), mapper.toExternal(second), mapper.toExternal(third));
        List<InvoiceFiles> retry = support.queryOneToMany(
                descriptor, new InvoiceBatchLookup("batch-failure.pdf"), InvoiceFiles.class,
                StreamedInvoiceFilesEntity.class, mapper)
            .collect().asList().await().indefinitely();

        assertEquals(List.of(first, second, third), retry);
    }

    private void replaceWith(Class<?> entityType, Object... entities) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        var transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            entityManager.createQuery("delete from " + entityType.getName()).executeUpdate();
            for (Object entity : entities) {
                entityManager.persist(entity);
            }
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

    private static InvoiceFiles invoice(String id, String filename, String suffix) {
        return new InvoiceFiles(
            UUID.fromString(id),
            filename,
            reference("invoice-" + suffix, "sha-" + suffix),
            reference("catalogue-" + suffix, "catalogue-sha-" + suffix));
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

    record InvoiceBatchLookup(String originalFilename) {
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

    static final class StreamedInvoiceFilesPersistenceMapper
        implements Mapper<InvoiceFiles, StreamedInvoiceFilesEntity> {
        private static final AtomicBoolean FAIL_ONCE_ON_SECOND = new AtomicBoolean();

        static void armPartialFailure() {
            FAIL_ONCE_ON_SECOND.set(true);
        }

        @Override
        public InvoiceFiles fromExternal(StreamedInvoiceFilesEntity external) {
            if (external.documentId.toString().endsWith("0002")
                && FAIL_ONCE_ON_SECOND.compareAndSet(true, false)) {
                throw new IllegalStateException("partial projection failure");
            }
            assertEquals("canonical-payloads", external.evidenceLabel);
            return new InvoiceFiles(
                external.documentId,
                external.originalFilename,
                PayloadReferenceProtobufCodec.decode(Base64.getDecoder().decode(external.invoiceReference)),
                PayloadReferenceProtobufCodec.decode(Base64.getDecoder().decode(external.catalogueReference)));
        }

        @Override
        public StreamedInvoiceFilesEntity toExternal(InvoiceFiles domain) {
            var entity = new StreamedInvoiceFilesEntity();
            entity.documentId = domain.documentId();
            entity.originalFilename = domain.originalFilename();
            entity.invoiceReference = Base64.getEncoder().encodeToString(
                PayloadReferenceProtobufCodec.encode(domain.invoice()));
            entity.catalogueReference = Base64.getEncoder().encodeToString(
                PayloadReferenceProtobufCodec.encode(domain.catalogue()));
            entity.evidenceLabel = "canonical-payloads";
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

    private static final class GeneratedJpaStreamingQueryStep extends ConfigurableStep
        implements StepOneToMany<CustomerRiskLookup, CustomerRiskFacts> {
        private final QueryStepSupport support;
        private final Uni<QueryStepDescriptor> descriptor;

        private GeneratedJpaStreamingQueryStep(QueryStepSupport support, Uni<QueryStepDescriptor> descriptor) {
            this.support = support;
            this.descriptor = descriptor;
        }

        @Override
        public Multi<CustomerRiskFacts> applyOneToMany(CustomerRiskLookup input) {
            return support.queryOneToMany(descriptor, input, CustomerRiskFacts.class);
        }
    }
}
