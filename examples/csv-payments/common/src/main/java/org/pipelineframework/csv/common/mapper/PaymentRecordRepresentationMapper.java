package org.pipelineframework.csv.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.mapper.Mapper;

/** Maps the canonical v3 payment record to the legacy JPA/OpenCSV representation. */
@ApplicationScoped
public class PaymentRecordRepresentationMapper
    implements Mapper<org.pipelineframework.csv.domain.PaymentRecord,
        org.pipelineframework.csv.common.domain.PaymentRecord> {

  @Override
  public org.pipelineframework.csv.domain.PaymentRecord fromExternal(
      org.pipelineframework.csv.common.domain.PaymentRecord external) {
    return new org.pipelineframework.csv.domain.PaymentRecord(
        external.getId(),
        external.getCsvId(),
        external.getRecipient(),
        external.getAmount(),
        external.getCurrency(),
        external.getCsvPaymentsInputFilePath());
  }

  @Override
  public org.pipelineframework.csv.common.domain.PaymentRecord toExternal(
      org.pipelineframework.csv.domain.PaymentRecord domain) {
    var representation = new org.pipelineframework.csv.common.domain.PaymentRecord();
    representation.setId(domain.id());
    representation.setCsvId(domain.csvId());
    representation.setRecipient(domain.recipient());
    representation.setAmount(domain.amount());
    representation.setCurrency(domain.currency());
    representation.setCsvPaymentsInputFilePath(domain.csvPaymentsInputFilePath());
    return representation;
  }
}
