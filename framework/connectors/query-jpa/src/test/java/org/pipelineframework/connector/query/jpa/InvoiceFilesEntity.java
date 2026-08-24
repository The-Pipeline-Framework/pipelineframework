package org.pipelineframework.connector.query.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.FetchType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/** Non-trivial persistence representation used to prove mapped JPA Query output. */
@Entity
public class InvoiceFilesEntity {
    static final AtomicReference<Thread> LAST_LOAD_THREAD = new AtomicReference<>();

    @Id
    public UUID documentId;

    @Column(nullable = false)
    public String originalFilename;

    @Column(nullable = false)
    public String invoiceReference;

    @Column(nullable = false)
    public String catalogueReference;

    @ElementCollection(fetch = FetchType.LAZY)
    public List<String> evidenceLabels = new ArrayList<>();

    protected InvoiceFilesEntity() {
    }

    @PostLoad
    void recordLoadThread() {
        LAST_LOAD_THREAD.set(Thread.currentThread());
    }
}
