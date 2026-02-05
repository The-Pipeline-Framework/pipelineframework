package org.pipelineframework.csv.process_send_payment_record.service;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import io.vertx.mutiny.core.Vertx;
import org.pipelineframework.csv.service.PaymentProviderServiceMock;

@Dependent
public class ProcessSendPaymentRecordService extends org.pipelineframework.csv.service.ProcessSendPaymentRecordService {
  @Inject
  public ProcessSendPaymentRecordService(PaymentProviderServiceMock paymentProviderServiceMock, Vertx vertx) {
    super(paymentProviderServiceMock, vertx);
  }
}
