package org.pipelineframework.csv.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.mapper.Mapper;

/** Maps canonical terminal output to the JPA/OpenCSV representation that owns its persistence id. */
@ApplicationScoped
public class PaymentOutputPersistenceMapper
    implements Mapper<org.pipelineframework.csv.domain.PaymentOutput,
        org.pipelineframework.csv.common.domain.PaymentOutput> {

  @Override
  public org.pipelineframework.csv.domain.PaymentOutput fromExternal(
      org.pipelineframework.csv.common.domain.PaymentOutput external) {
    return new org.pipelineframework.csv.domain.PaymentOutput(
        external.getCsvPaymentsOutputFilename(),
        external.getCsvPaymentsInputFilePath(),
        external.getCsvId(),
        external.getRecipient(),
        external.getAmount(),
        external.getCurrency(),
        external.getConversationId(),
        external.getStatus(),
        external.getMessage(),
        external.getFee());
  }

  @Override
  public org.pipelineframework.csv.common.domain.PaymentOutput toExternal(
      org.pipelineframework.csv.domain.PaymentOutput domain) {
    var representation = new org.pipelineframework.csv.common.domain.PaymentOutput();
    representation.setCsvPaymentsOutputFilename(domain.csvPaymentsOutputFilename());
    representation.setCsvPaymentsInputFilePath(domain.csvPaymentsInputFilePath());
    representation.setCsvId(domain.csvId());
    representation.setRecipient(domain.recipient());
    representation.setAmount(domain.amount());
    representation.setCurrency(domain.currency());
    representation.setConversationId(domain.conversationId());
    representation.setStatus(domain.status());
    representation.setMessage(domain.message());
    representation.setFee(domain.fee());
    return representation;
  }
}
