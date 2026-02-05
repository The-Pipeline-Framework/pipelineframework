package org.pipelineframework.csv.process_ack_payment_sent.service;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.pipelineframework.csv.service.PollAckPaymentSentService;

@Dependent
public class ProcessAckPaymentSentService extends org.pipelineframework.csv.service.ProcessAckPaymentSentService {
  @Inject
  public ProcessAckPaymentSentService(PollAckPaymentSentService pollAckPaymentSentService) {
    super(pollAckPaymentSentService);
  }
}
