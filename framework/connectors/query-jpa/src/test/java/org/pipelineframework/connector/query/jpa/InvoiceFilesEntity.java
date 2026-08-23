package org.pipelineframework.connector.query.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.util.UUID;

/** Non-trivial persistence representation used to prove mapped JPA Query output. */
@Entity
public class InvoiceFilesEntity {
    @Id
    public UUID documentId;

    @Column(nullable = false)
    public String originalFilename;

    @Column(nullable = false)
    public String invoiceReference;

    @Column(nullable = false)
    public String catalogueReference;

    protected InvoiceFilesEntity() {
    }
}
