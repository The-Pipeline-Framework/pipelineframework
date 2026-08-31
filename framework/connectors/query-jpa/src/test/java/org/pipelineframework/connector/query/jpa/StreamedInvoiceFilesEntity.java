package org.pipelineframework.connector.query.jpa;

import java.util.UUID;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class StreamedInvoiceFilesEntity {
    @Id
    public UUID documentId;

    public String originalFilename;
    public String invoiceReference;
    public String catalogueReference;
    public String evidenceLabel;
}
